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
  };
}

const EXCLUDED_TABLES = new Set([
  "cache",
  "cache_locks",
  "failed_jobs",
  "job_batches",
  "jobs",
  "migrations",
  "password_reset_tokens",
  "personal_access_tokens",
  "sessions",
  "sym_change_log",
  "sym_data",
]);

const localDbConfig = parseConnectionString(process.env.LOCAL_URL);
const localDb = knex({
  client: "mysql2",
  connection: localDbConfig,
});

const branchConfigs = JSON.parse(process.env.BRANCHES_JSON || "[]");
const branchDbs = branchConfigs.map((b) => ({
  id: b.id,
  dbConfig: parseConnectionString(b.url),
  db: knex({
    client: "mysql2",
    connection: parseConnectionString(b.url),
  }),
}));

async function getTargetTables(db, dbName) {
  const [rows] = await db.raw(
    `SELECT TABLE_NAME 
     FROM INFORMATION_SCHEMA.TABLES 
     WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'`,
    [dbName],
  );

  return rows
    .map((r) => r.TABLE_NAME)
    .filter((table) => !EXCLUDED_TABLES.has(table));
}

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

async function attachTriggersForNode(nodeId, db, dbName) {
  console.log(`\n--- Provisioning Triggers for Node: [${nodeId}] ---`);

  const tables = await getTargetTables(db, dbName);

  // Process tables concurrently
  await Promise.all(
    tables.map(async (table) => {
      try {
        const { pkCol, colNames } = await getPrimaryKeyAndColumns(db, table);

        const jsonNewPairList = colNames
          .map((col) => `'${col}', NEW.\`${col}\``)
          .join(", ");

        const jsonOldPairList = colNames
          .map((col) => `'${col}', OLD.\`${col}\``)
          .join(", ");

        const insertTriggerSql = `
          CREATE TRIGGER IF NOT EXISTS \`trg_${table}_ai\`
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
          CREATE TRIGGER IF NOT EXISTS \`trg_${table}_au\`
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
          CREATE TRIGGER IF NOT EXISTS \`trg_${table}_ad\`
          AFTER DELETE ON \`${table}\`
          FOR EACH ROW
          BEGIN
            IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
              INSERT INTO \`sym_change_log\` (table_name, operation, primary_key_val, row_data, node_source_id, created_at)
              VALUES ('${table}', 'DELETE', CAST(OLD.\`${pkCol}\` AS CHAR), JSON_OBJECT(${jsonOldPairList}), '${nodeId}', NOW());
            END IF;
          END;
        `;

        await db.raw(`DROP TRIGGER IF EXISTS \`trg_${table}_ai\``);
        await db.raw(`DROP TRIGGER IF EXISTS \`trg_${table}_au\``);
        await db.raw(`DROP TRIGGER IF EXISTS \`trg_${table}_ad\``);

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
    }),
  );
}

async function main() {
  const hubId = process.env.LOCAL_ID || "NODE_MAIN_HUB";
  await attachTriggersForNode(hubId, localDb, localDbConfig.database);

  for (const branch of branchDbs) {
    await attachTriggersForNode(branch.id, branch.db, branch.dbConfig.database);
  }

  await localDb.destroy();
  for (const b of branchDbs) await b.db.destroy();

  console.log("\nTrigger provisioning completed successfully!");
}

main().catch(console.error);
