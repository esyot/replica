require("dotenv").config();
const knex = require("knex");

const rawAllowedTables = process.env.ALLOWED_TABLES || "";
const ALLOWED_TABLES = new Set(
  rawAllowedTables
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean),
);

const pkCache = new Map();

async function getTablePkColumns(node, tableName) {
  const cacheKey = `${node.id}:${tableName}`;
  if (pkCache.has(cacheKey)) {
    return pkCache.get(cacheKey);
  }

  const db = node.db;
  const client = node.client;
  let pkColumns = [];

  if (client === "pg" || client === "postgres") {
    const res = await db.raw(
      `SELECT a.attname
       FROM pg_index i
       JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
       WHERE i.indrelid = ?::regclass AND i.indisprimary;`,
      [tableName],
    );
    pkColumns = res.rows.map((r) => r.attname);
  } else {
    const res = await db.raw(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [
      tableName,
    ]);
    const rows = res[0] || res;
    pkColumns = rows.map((r) => r.Column_name);
  }

  if (pkColumns.length > 0) {
    pkCache.set(cacheKey, pkColumns);
  }

  return pkColumns;
}

async function getTableColumns(node, tableName) {
  const db = node.db;
  const client = node.client;

  if (client === "pg" || client === "postgres") {
    const res = await db.raw(
      `SELECT column_name FROM information_schema.columns WHERE table_name = ?;`,
      [tableName],
    );
    return res.rows.map((r) => r.column_name);
  } else {
    const res = await db.raw(`SHOW COLUMNS FROM ??`, [tableName]);
    const rows = res[0] || res;
    return rows.map((r) => r.Field);
  }
}

async function setupNodeTriggers(node) {
  const db = node.db;
  const client = node.client;
  const isPg = client === "pg" || client === "postgres";

  console.log(
    `[Triggers] Installing triggers on node [${node.id}] (${client}) for ${ALLOWED_TABLES.size} table(s)...`,
  );

  for (const tableName of ALLOWED_TABLES) {
    try {
      const pkColumns = await getTablePkColumns(node, tableName);
      if (!pkColumns || pkColumns.length === 0) {
        console.warn(
          `[Triggers] Skipping '${tableName}' on [${node.id}]: No Primary Key found.`,
        );
        continue;
      }

      const columns = await getTableColumns(node, tableName);

      if (isPg) {
        await setupPostgresTriggers(db, node.id, tableName, pkColumns, columns);
      } else {
        await setupMysqlTriggers(db, node.id, tableName, pkColumns, columns);
      }
    } catch (err) {
      console.error(
        `[Triggers] Error generating triggers for '${tableName}' on [${node.id}]:`,
        err.message,
      );
    }
  }
}

async function setupMysqlTriggers(db, nodeId, tableName, pkColumns, columns) {
  const buildPkVal = (prefix) => {
    if (pkColumns.length === 1) {
      return `CAST(${prefix}.${pkColumns[0]} AS CHAR)`;
    }
    return `CONCAT_WS('-', ${pkColumns.map((col) => `CAST(${prefix}.${col} AS CHAR)`).join(", ")})`;
  };

  const jsonFields = columns
    .map(
      (col) =>
        `'${col}', ${col === "updated_at" || col === "created_at" ? `DATE_FORMAT(NEW.${col}, '%Y-%m-%d %H:%i:%s')` : `NEW.${col}`}`,
    )
    .join(", ");

  const jsonFieldsOld = columns
    .map(
      (col) =>
        `'${col}', ${col === "updated_at" || col === "created_at" ? `DATE_FORMAT(OLD.${col}, '%Y-%m-%d %H:%i:%s')` : `OLD.${col}`}`,
    )
    .join(", ");

  const triggerInsert = `sym_trig_${tableName}_ins`;
  const triggerUpdate = `sym_trig_${tableName}_upd`;
  const triggerDelete = `sym_trig_${tableName}_del`;

  await db.raw(`DROP TRIGGER IF EXISTS ??`, [triggerInsert]);
  await db.raw(`DROP TRIGGER IF EXISTS ??`, [triggerUpdate]);
  await db.raw(`DROP TRIGGER IF EXISTS ??`, [triggerDelete]);

  // INSERT TRIGGER
  await db.raw(
    `
    CREATE TRIGGER ?? 
    AFTER INSERT ON ??
    FOR EACH ROW
    BEGIN
      IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES ('${tableName}', 'INSERT', ${buildPkVal("NEW")}, JSON_OBJECT(${jsonFields}), '${nodeId}');
      END IF;
    END;
  `,
    [triggerInsert, tableName],
  );

  // UPDATE TRIGGER
  await db.raw(
    `
    CREATE TRIGGER ?? 
    AFTER UPDATE ON ??
    FOR EACH ROW
    BEGIN
      IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES ('${tableName}', 'UPDATE', ${buildPkVal("NEW")}, JSON_OBJECT(${jsonFields}), '${nodeId}');
      END IF;
    END;
  `,
    [triggerUpdate, tableName],
  );

  // DELETE TRIGGER
  await db.raw(
    `
    CREATE TRIGGER ?? 
    AFTER DELETE ON ??
    FOR EACH ROW
    BEGIN
      IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES ('${tableName}', 'DELETE', ${buildPkVal("OLD")}, JSON_OBJECT(${jsonFieldsOld}), '${nodeId}');
      END IF;
    END;
  `,
    [triggerDelete, tableName],
  );
}

async function setupPostgresTriggers(
  db,
  nodeId,
  tableName,
  pkColumns,
  columns,
) {
  const funcName = `sym_fn_${tableName}_change`;
  const triggerName = `sym_trig_${tableName}`;

  await db.raw(`DROP TRIGGER IF EXISTS ?? ON ??;`, [triggerName, tableName]);
  await db.raw(`DROP FUNCTION IF EXISTS ??();`, [funcName]);

  const pkBuild =
    pkColumns.length === 1
      ? `CAST(target_record.${pkColumns[0]} AS TEXT)`
      : pkColumns
          .map((col) => `CAST(target_record.${col} AS TEXT)`)
          .join(" || '-' || ");

  await db.raw(`
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS $$
    DECLARE
      is_syncing TEXT;
      target_record RECORD;
      pk_val TEXT;
      row_json TEXT;
    BEGIN
      BEGIN
        is_syncing := current_setting('sym.is_syncing', true);
      EXCEPTION WHEN OTHERS THEN
        is_syncing := 'false';
      END;

      IF is_syncing IS NULL OR is_syncing != 'true' THEN
        IF (TG_OP = 'DELETE') THEN
          target_record := OLD;
        ELSE
          target_record := NEW;
        END IF;

        pk_val := ${pkBuild};
        row_json := row_to_json(target_record)::text;

        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES ('${tableName}', TG_OP, pk_val, row_json, '${nodeId}');
      END IF;

      RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
  `);

  await db.raw(
    `
    CREATE TRIGGER ${triggerName}
    AFTER INSERT OR UPDATE OR DELETE ON ??
    FOR EACH ROW EXECUTE FUNCTION ${funcName}();
  `,
    [tableName],
  );
}

module.exports = {
  getTablePkColumns,
  setupNodeTriggers,
  ALLOWED_TABLES,
};

if (require.main === module) {
  function parseConnectionString(urlStr) {
    if (!urlStr) throw new Error("empty connection string");
    const parsed = new URL(urlStr);
    return {
      host: parsed.hostname || "127.0.0.1",
      port: parsed.port ? parseInt(parsed.port, 10) : 3306,
      user: parsed.username ? decodeURIComponent(parsed.username) : undefined,
      password: parsed.password
        ? decodeURIComponent(parsed.password)
        : undefined,
      database: parsed.pathname.replace("/", ""),
    };
  }

  function normalizeClient(client) {
    if (!client) return "mysql2";
    const c = client.toLowerCase().trim();
    return c === "mysql" ? "mysql2" : c;
  }

  async function run() {
    const nodesToSetup = [];

    if (process.env.LOCAL_URL) {
      const hubClient = normalizeClient(process.env.LOCAL_CLIENT);
      nodesToSetup.push({
        id: process.env.LOCAL_ID || "NODE_MAIN_HUB",
        client: hubClient,
        url: process.env.LOCAL_URL,
        ssl: process.env.LOCAL_SSL === "true",
        sslStrict: process.env.LOCAL_SSL_STRICT !== "false",
      });
    }

    let branches = [];
    try {
      branches = JSON.parse(process.env.BRANCHES_JSON || "[]");
    } catch (err) {
      console.error("Failed to parse BRANCHES_JSON:", err.message);
    }

    for (const b of branches) {
      const branchClient = normalizeClient(b.client);
      nodesToSetup.push({
        id: b.id,
        client: branchClient,
        url: b.url,
        ssl: b.sslInsecure !== true,
        sslStrict: true,
      });
    }

    if (nodesToSetup.length === 0) {
      console.warn("No database connection strings found in .env.");
      return;
    }

    for (const n of nodesToSetup) {
      const isPg = n.client === "pg" || n.client === "postgres";
      const db = knex({
        client: n.client,
        connection: isPg
          ? {
              connectionString: n.url,
              ssl: n.ssl ? { rejectUnauthorized: n.sslStrict } : false,
            }
          : parseConnectionString(n.url),
      });

      const node = { id: n.id, client: n.client, db };
      console.log(`Setting up triggers for node [${node.id}]...`);
      try {
        await setupNodeTriggers(node);
        console.log(`Successfully configured triggers for [${node.id}]`);
      } catch (err) {
        console.error(
          `Failed to set up triggers for [${node.id}]:`,
          err.message,
        );
      } finally {
        await db.destroy();
      }
    }

    console.log("\nAll trigger setups completed.");
  }

  run().catch((err) => {
    console.error("Fatal error during trigger setup:", err);
    process.exit(1);
  });
}
