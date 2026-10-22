require("dotenv").config();
const knex = require("knex");

/* ------------------------------------------------------------------ */
/* Logging                                                            */
/* ------------------------------------------------------------------ */

// This module is imported by the sync engine (which has winston) and also
// run standalone. We accept an injected logger, default to console.
function defaultLogger() {
  return {
    info: (...a) => console.log(...a),
    warn: (...a) => console.warn(...a),
    error: (...a) => console.error(...a),
    debug: (...a) => {
      if (process.env.NODE_ENV !== "production") console.debug(...a);
    },
  };
}

let LOGGER = defaultLogger();
function setLogger(l) {
  if (l) LOGGER = l;
}

/* ------------------------------------------------------------------ */
/* Table allowlist                                                    */
/* ------------------------------------------------------------------ */

const rawAllowedTables = process.env.ALLOWED_TABLES || "";
const ALLOWED_TABLES = new Set(
  rawAllowedTables
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean),
);

/* ------------------------------------------------------------------ */
/* Identifier validation                                              */
/* ------------------------------------------------------------------ */

// Conservative: letters, digits, underscore. No dots — schema is scoped
// separately via current_schema()/DATABASE(). Length 1..63 (PG's limit).
const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

function assertSafeIdentifier(name, kind) {
  if (typeof name !== "string" || !IDENT_RE.test(name)) {
    throw new Error(
      `Unsafe ${kind} identifier ${JSON.stringify(name)}: must match ${IDENT_RE}`,
    );
  }
  return name;
}

function quoteMysqlIdent(name) {
  assertSafeIdentifier(name, "MySQL");
  return `\`${name}\``;
}

function quotePgIdent(name) {
  assertSafeIdentifier(name, "PostgreSQL");
  return `"${name}"`;
}

function escapeSqlString(s) {
  return String(s).replace(/'/g, "''");
}

/* ------------------------------------------------------------------ */
/* PK cache (promise-cached)                                          */
/* ------------------------------------------------------------------ */

const pkCache = new Map(); // key -> Promise<string[]>

async function getTablePkColumns(node, tableName) {
  assertSafeIdentifier(tableName, "table");
  const cacheKey = `${node.id}:${tableName}`;
  if (pkCache.has(cacheKey)) return pkCache.get(cacheKey);

  const promise = (async () => {
    const db = node.db;
    const client = node.client;
    let pkColumns = [];

    if (client === "pg" || client === "postgres") {
      // WITH ORDINALITY preserves index column order — critical for
      // composite PKs. `attnum = ANY(indkey)` alone does not guarantee order.
      const res = await db.raw(
        `SELECT a.attname
         FROM pg_index i
         JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
         WHERE i.indrelid = ?::regclass AND i.indisprimary
         ORDER BY k.ord;`,
        [tableName],
      );
      pkColumns = res.rows.map((r) => r.attname);
    } else {
      // MySQL SHOW KEYS does not guarantee order; sort by Seq_in_index.
      const res = await db.raw(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [
        tableName,
      ]);
      const rows = res[0] || res;
      pkColumns = rows
        .slice()
        .sort((a, b) => Number(a.Seq_in_index) - Number(b.Seq_in_index))
        .map((r) => r.Column_name);
    }

    return pkColumns;
  })();

  // Don't cache failures.
  promise.catch(() => pkCache.delete(cacheKey));
  pkCache.set(cacheKey, promise);
  return promise;
}

function clearPkCache(nodeId) {
  if (!nodeId) {
    pkCache.clear();
    return;
  }
  for (const key of pkCache.keys()) {
    if (key.startsWith(`${nodeId}:`)) pkCache.delete(key);
  }
}

/* ------------------------------------------------------------------ */
/* Column discovery (schema-scoped)                                   */
/* ------------------------------------------------------------------ */

async function getTableColumns(node, tableName) {
  assertSafeIdentifier(tableName, "table");
  const db = node.db;
  const client = node.client;

  if (client === "pg" || client === "postgres") {
    const res = await db.raw(
      `SELECT column_name
         FROM information_schema.columns
        WHERE table_name = ? AND table_schema = current_schema()
        ORDER BY ordinal_position;`,
      [tableName],
    );
    return res.rows.map((r) => r.column_name);
  }

  // MySQL: scope to the connection's default database.
  const res = await db.raw(
    `SELECT COLUMN_NAME
       FROM information_schema.columns
      WHERE table_name = ? AND table_schema = DATABASE()
      ORDER BY ORDINAL_POSITION;`,
    [tableName],
  );
  const rows = res[0] || res;
  return rows.map((r) => r.COLUMN_NAME);
}

/* ------------------------------------------------------------------ */
/* Trigger setup                                                      */
/* ------------------------------------------------------------------ */

async function setupNodeTriggers(node, opts = {}) {
  const db = node.db;
  const client = node.client;
  const isPg = client === "pg" || client === "postgres";
  const nodeId = String(node.id);

  assertSafeIdentifier(nodeId, "node id");

  const summary = { ok: [], failed: [], skipped: [] };

  LOGGER.info(
    `[Triggers] Installing triggers on node [${node.id}] (${client}) for ${ALLOWED_TABLES.size} table(s)...`,
  );

  for (const tableName of ALLOWED_TABLES) {
    try {
      assertSafeIdentifier(tableName, "table");

      const pkColumns = await getTablePkColumns(node, tableName);
      if (!pkColumns || pkColumns.length === 0) {
        LOGGER.warn(
          `[Triggers] Skipping '${tableName}' on [${node.id}]: No Primary Key found.`,
        );
        summary.skipped.push({ table: tableName, reason: "no primary key" });
        continue;
      }

      const columns = await getTableColumns(node, tableName);
      if (columns.length === 0) {
        LOGGER.warn(
          `[Triggers] Skipping '${tableName}' on [${node.id}]: table not found in current schema.`,
        );
        summary.skipped.push({ table: tableName, reason: "table not found" });
        continue;
      }

      if (isPg) {
        await setupPostgresTriggers(db, nodeId, tableName, pkColumns, columns);
      } else {
        await setupMysqlTriggers(db, nodeId, tableName, pkColumns, columns);
      }

      LOGGER.info(
        `[Triggers] '${tableName}' on [${node.id}]: PK=${JSON.stringify(pkColumns)} ${
          pkColumns.length > 1
            ? "(composite, JSON-array encoded)"
            : "(single-column)"
        }`,
      );
      summary.ok.push({ table: tableName, pkColumns });
    } catch (err) {
      LOGGER.error(
        `[Triggers] Error generating triggers for '${tableName}' on [${node.id}]: ${err.message}`,
      );
      summary.failed.push({ table: tableName, error: err.message });
    }
  }

  return summary;
}

/* ------------------------------------------------------------------ */
/* Allowed columns per table (trigger-side filter)                    */
/* ------------------------------------------------------------------ */

function getAllowedColumnsForTable(tableName) {
  try {
    const raw = process.env.ALLOWED_COLUMNS_JSON;
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const list = parsed[tableName];
    if (!Array.isArray(list) || list.length === 0) return null;
    return list;
  } catch {
    return null;
  }
}

/* ------------------------------------------------------------------ */
/* MySQL trigger SQL                                                  */
/* ------------------------------------------------------------------ */

async function setupMysqlTriggers(
  db,
  nodeId,
  tableName,
  pkColumns,
  allColumns,
) {
  // Enforce ALLOWED_COLUMNS at trigger time when configured, so disallowed
  // column values never enter sym_change_log.
  const allowList = getAllowedColumnsForTable(tableName);
  const columns = allowList
    ? allColumns.filter((c) => allowList.includes(c))
    : allColumns;

  if (columns.length === 0) {
    throw new Error(
      `No columns to serialize for '${tableName}' (ALLOWED_COLUMNS_JSON filtered everything).`,
    );
  }

  const buildPkVal = (prefix) => {
    if (pkColumns.length === 1) {
      return `CAST(${prefix}.${quoteMysqlIdent(pkColumns[0])} AS CHAR)`;
    }
    return `CAST(JSON_ARRAY(${pkColumns
      .map((col) => `CAST(${prefix}.${quoteMysqlIdent(col)} AS CHAR)`)
      .join(", ")}) AS CHAR)`;
  };

  const buildJsonObject = (prefix) => {
    const pairs = columns.map((col) => {
      const ident = quoteMysqlIdent(col);
      const valueExpr =
        col === "updated_at" || col === "created_at"
          ? `DATE_FORMAT(${prefix}.${ident}, '%Y-%m-%d %H:%i:%s')`
          : `${prefix}.${ident}`;
      return `'${escapeSqlString(col)}', ${valueExpr}`;
    });
    return `JSON_OBJECT(${pairs.join(", ")})`;
  };

  const tableIdent = quoteMysqlIdent(tableName);
  const sourceLiteral = `'${escapeSqlString(nodeId)}'`;
  const tableLiteral = `'${escapeSqlString(tableName)}'`;

  const triggerInsert = `sym_trig_${tableName}_ins`;
  const triggerUpdate = `sym_trig_${tableName}_upd`;
  const triggerDelete = `sym_trig_${tableName}_del`;

  await db.raw(`DROP TRIGGER IF EXISTS ??`, [triggerInsert]);
  await db.raw(`DROP TRIGGER IF EXISTS ??`, [triggerUpdate]);
  await db.raw(`DROP TRIGGER IF EXISTS ??`, [triggerDelete]);

  const mkTrigger = (name, op, newOrOld) => `
    CREATE TRIGGER ${quoteMysqlIdent(name)}
    AFTER ${op} ON ${tableIdent}
    FOR EACH ROW
    BEGIN
      IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES (${tableLiteral}, '${op}', ${buildPkVal(newOrOld)}, ${buildJsonObject(newOrOld)}, ${sourceLiteral});
      END IF;
    END;
  `;

  try {
    await db.raw(mkTrigger(triggerInsert, "INSERT", "NEW"));
    await db.raw(mkTrigger(triggerUpdate, "UPDATE", "NEW"));
    await db.raw(mkTrigger(triggerDelete, "DELETE", "OLD"));
  } catch (err) {
    if (
      /ER_BINLOG_CREATE_ROUTINE_NEED_SUPER|SUPER privilege/i.test(
        err.message,
      ) ||
      err.code === "ER_BINLOG_CREATE_ROUTINE_NEED_SUPER"
    ) {
      throw new Error(
        `${err.message}\n` +
          `Hint: MySQL binlog is enabled and this account lacks SUPER. ` +
          `Run "SET GLOBAL log_bin_trust_function_creators = 1;" as an admin, or grant SUPER.`,
      );
    }
    throw err;
  }
}

/* ------------------------------------------------------------------ */
/* PostgreSQL trigger SQL                                             */
/* ------------------------------------------------------------------ */

async function setupPostgresTriggers(
  db,
  nodeId,
  tableName,
  pkColumns,
  allColumns,
) {
  const allowList = getAllowedColumnsForTable(tableName);
  const columns = allowList
    ? allColumns.filter((c) => allowList.includes(c))
    : allColumns;

  if (columns.length === 0) {
    throw new Error(
      `No columns to serialize for '${tableName}' (ALLOWED_COLUMNS_JSON filtered everything).`,
    );
  }

  const funcName = `sym_fn_${tableName}_change`;
  const triggerName = `sym_trig_${tableName}`;

  // Function/trigger names are derived from a validated identifier so the
  // derived name is also safe.
  assertSafeIdentifier(funcName, "function");
  assertSafeIdentifier(triggerName, "trigger");

  const funcIdent = quotePgIdent(funcName);
  const triggerIdent = quotePgIdent(triggerName);
  const tableIdent = quotePgIdent(tableName);

  await db.raw(`DROP TRIGGER IF EXISTS ${triggerIdent} ON ${tableIdent};`);
  await db.raw(`DROP FUNCTION IF EXISTS ${funcIdent}();`);

  const pkBuild =
    pkColumns.length === 1
      ? `CAST(target_record.${quotePgIdent(pkColumns[0])} AS TEXT)`
      : `json_build_array(${pkColumns
          .map((col) => `CAST(target_record.${quotePgIdent(col)} AS TEXT)`)
          .join(", ")})::text`;

  // Build row_json from only the allowed columns (or all, if no allowlist).
  const rowJsonExpr =
    columns.length > 0 && allowList
      ? `json_build_object(${columns
          .map(
            (col) =>
              `'${escapeSqlString(col)}', target_record.${quotePgIdent(col)}`,
          )
          .join(", ")})::text`
      : `row_to_json(target_record)::text`;

  const sourceLiteral = `'${escapeSqlString(nodeId)}'`;
  const tableLiteral = `'${escapeSqlString(tableName)}'`;

  const fnBody = `
    CREATE OR REPLACE FUNCTION ${funcIdent}()
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
        row_json := ${rowJsonExpr};

        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES (${tableLiteral}, TG_OP, pk_val, row_json, ${sourceLiteral});
      END IF;

      RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
  `;

  await db.raw(fnBody);

  await db.raw(`
    CREATE TRIGGER ${triggerIdent}
    AFTER INSERT OR UPDATE OR DELETE ON ${tableIdent}
    FOR EACH ROW EXECUTE FUNCTION ${funcIdent}();
  `);
}

/* ------------------------------------------------------------------ */
/* Exports                                                            */
/* ------------------------------------------------------------------ */

module.exports = {
  getTablePkColumns,
  getTableColumns,
  setupNodeTriggers,
  clearPkCache,
  setLogger,
  ALLOWED_TABLES,
};

/* ------------------------------------------------------------------ */
/* Standalone CLI                                                     */
/* ------------------------------------------------------------------ */

if (require.main === module) {
  (async () => {
    const args = process.argv.slice(2);

    if (args.includes("--help") || args.includes("-h")) {
      console.log(`
Usage:
  node triggers.js [--node=ID] [--table=NAME]

Env (as before):
  ALLOWED_TABLES        comma-separated table names
  ALLOWED_COLUMNS_JSON  optional JSON: {"table": ["col1","col2"]}
  LOCAL_URL / LOCAL_CLIENT / LOCAL_ID / LOCAL_SSL / LOCAL_SSL_STRICT
  BRANCHES_JSON         array of { id, url, client?, sslInsecure? }

Flags:
  --node=ID     only set up triggers for this node
  --table=NAME  only set up triggers for this table (on all selected nodes)
`);
      process.exit(0);
    }

    const flag = (name) => {
      const hit = args.find((a) => a.startsWith(`--${name}=`));
      return hit ? hit.slice(name.length + 3) : null;
    };

    const onlyNode = flag("node");
    const onlyTable = flag("table");

    if (onlyTable) {
      // Narrow ALLOWED_TABLES for this run only.
      for (const t of Array.from(ALLOWED_TABLES)) {
        if (t !== onlyTable) ALLOWED_TABLES.delete(t);
      }
      if (ALLOWED_TABLES.size === 0) {
        console.error(`--table=${onlyTable} is not in ALLOWED_TABLES.`);
        process.exit(1);
      }
    }

    function parseConnectionString(urlStr, defaultPort) {
      if (!urlStr) throw new Error("empty connection string");
      const parsed = new URL(urlStr);
      return {
        host: parsed.hostname || "127.0.0.1",
        port: parsed.port ? parseInt(parsed.port, 10) : defaultPort,
        user: parsed.username ? decodeURIComponent(parsed.username) : undefined,
        password: parsed.password
          ? decodeURIComponent(parsed.password)
          : undefined,
        database: parsed.pathname.slice(1),
      };
    }

    function normalizeClient(client) {
      if (!client) return "mysql2";
      const c = client.toLowerCase().trim();
      if (c === "mysql") return "mysql2";
      if (c === "postgresql") return "pg";
      return c;
    }

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
        ssl: true,
        sslStrict: b.sslInsecure !== true,
      });
    }

    if (nodesToSetup.length === 0) {
      console.warn("No database connection strings found in .env.");
      process.exit(0);
    }

    let anyFailed = false;

    for (const n of nodesToSetup) {
      if (onlyNode && n.id !== onlyNode) continue;

      const isPg = n.client === "pg" || n.client === "postgres";
      const db = knex({
        client: n.client,
        connection: isPg
          ? {
              connectionString: n.url,
              ssl: n.ssl ? { rejectUnauthorized: n.sslStrict } : false,
            }
          : parseConnectionString(n.url, 3306),
        pool: { min: 0, max: 4 },
      });

      const node = { id: n.id, client: n.client, db };
      console.log(`Setting up triggers for node [${node.id}]...`);
      try {
        const summary = await setupNodeTriggers(node);
        console.log(
          `Node [${node.id}] done: ${summary.ok.length} ok, ` +
            `${summary.skipped.length} skipped, ${summary.failed.length} failed.`,
        );
        if (summary.failed.length > 0) {
          anyFailed = true;
          for (const f of summary.failed) {
            console.error(`  FAILED ${f.table}: ${f.error}`);
          }
        }
      } catch (err) {
        anyFailed = true;
        console.error(
          `Failed to set up triggers for [${node.id}]: ${err.message}`,
        );
      } finally {
        await db.destroy();
      }
    }

    console.log("\nAll trigger setups completed.");
    process.exit(anyFailed ? 1 : 0);
  })().catch((err) => {
    console.error("Fatal error during trigger setup:", err);
    process.exit(1);
  });
}
