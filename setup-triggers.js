require("dotenv").config();
const knex = require("knex");

function parseConnectionString(urlStr) {
  if (!urlStr) throw new Error("Empty connection string");
  const parsed = new URL(urlStr);
  return {
    host: parsed.hostname || "127.0.0.1",
    port: parsed.port ? parseInt(parsed.port, 10) : 3306,
    user: parsed.username ? decodeURIComponent(parsed.username) : undefined,
    password: parsed.password ? decodeURIComponent(parsed.password) : undefined,
    database: parsed.pathname.replace("/", ""),
    multipleStatements: true, // Enabled for multi-line trigger creation
  };
}

const ALLOWED_TABLES = Array.from(
  new Set(
    (process.env.ALLOWED_TABLES || "")
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean),
  ),
);

const localDb = knex({
  client: "mysql2",
  connection: parseConnectionString(process.env.LOCAL_URL),
});

const branchConfigs = JSON.parse(process.env.BRANCHES_JSON || "[]");
const branchDbs = branchConfigs.map((b) => ({
  id: b.id,
  db: knex({
    client: "mysql2",
    connection: parseConnectionString(b.url),
  }),
}));

async function getPrimaryKeyAndColumns(db, tableName) {
  const columns = await db.raw(`SHOW COLUMNS FROM \`${tableName}\``);
  const cols = columns[0];

  let pkCol = "id";
  const colNames = [];

  for (const col of cols) {
    colNames.push(col.Field);
    if (col.Key === "PRI") {
      pkCol = col.Field;
    }
  }

  return { pkCol, colNames };
}

async function attachTriggersForNode(nodeId, db) {
  console.log(`\n--- Provisioning Triggers for Node: [${nodeId}] ---`);

  if (ALLOWED_TABLES.length === 0) {
    console.warn("[Warning] ALLOWED_TABLES is empty in .env!");
    return;
  }

  for (const table of ALLOWED_TABLES) {
    try {
      const hasTable = await db.schema.hasTable(table);
      if (!hasTable) {
        console.warn(`[Skip] Table '${table}' does not exist on ${nodeId}`);
        continue;
      }

      const { pkCol, colNames } = await getPrimaryKeyAndColumns(db, table);

      // Construct JSON payload string for MySQL JSON_OBJECT(col1, NEW.col1, ...)
      const jsonNewPairList = colNames
        .map((col) => `'${col}', NEW.\`${col}\``)
        .join(", ");

      const jsonOldPairList = colNames
        .map((col) => `'${col}', OLD.\`${col}\``)
        .join(", ");

      const insertTriggerSql = `
        CREATE TRIGGER \`trg_${table}_ai\`
        AFTER INSERT ON \`${table}\`
        FOR EACH ROW
        BEGIN
          IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
            INSERT INTO \`sym_change_log\` (table_name, operation, primary_key_val, row_data, node_source_id, created_at)
            VALUES ('${table}', 'INSERT', CAST(NEW.\`${pkCol}\` AS CHAR), JSON_OBJECT(${jsonNewPairList}), '${nodeId}', NOW());
          END IF;
        END;
      `;

      const updateTriggerSql = `
        CREATE TRIGGER \`trg_${table}_au\`
        AFTER UPDATE ON \`${table}\`
        FOR EACH ROW
        BEGIN
          IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
            INSERT INTO \`sym_change_log\` (table_name, operation, primary_key_val, row_data, node_source_id, created_at)
            VALUES ('${table}', 'UPDATE', CAST(NEW.\`${pkCol}\` AS CHAR), JSON_OBJECT(${jsonNewPairList}), '${nodeId}', NOW());
          END IF;
        END;
      `;

      const deleteTriggerSql = `
        CREATE TRIGGER \`trg_${table}_ad\`
        AFTER DELETE ON \`${table}\`
        FOR EACH ROW
        BEGIN
          IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
            INSERT INTO \`sym_change_log\` (table_name, operation, primary_key_val, row_data, node_source_id, created_at)
            VALUES ('${table}', 'DELETE', CAST(OLD.\`${pkCol}\` AS CHAR), JSON_OBJECT(${jsonOldPairList}), '${nodeId}', NOW());
          END IF;
        END;
      `;

      // 1. Drop existing triggers
      await db.raw(`DROP TRIGGER IF EXISTS \`trg_${table}_ai\``);
      await db.raw(`DROP TRIGGER IF EXISTS \`trg_${table}_au\``);
      await db.raw(`DROP TRIGGER IF EXISTS \`trg_${table}_ad\``);

      // 2. Create new triggers without IF NOT EXISTS
      await db.raw(insertTriggerSql);
      await db.raw(updateTriggerSql);
      await db.raw(deleteTriggerSql);

      console.log(`[Success] Triggers attached to table: '${table}'`);
    } catch (err) {
      console.error(
        `[Error] Failed to create triggers for table '${table}' on ${nodeId}:`,
        err.message,
      );
    }
  }
}

async function main() {
  const hubId = process.env.LOCAL_ID || "NODE_MAIN_HUB";
  await attachTriggersForNode(hubId, localDb);

  for (const branch of branchDbs) {
    await attachTriggersForNode(branch.id, branch.db);
  }

  await localDb.destroy();
  for (const b of branchDbs) await b.db.destroy();

  console.log("\nTrigger provisioning completed successfully!");
}

main().catch(console.error);
