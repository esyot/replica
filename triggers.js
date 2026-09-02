const winston = require("winston");

const logger = winston.createLogger({
  level: process.env.NODE_ENV === "production" ? "info" : "debug",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      ({ timestamp, level, message }) =>
        `[${timestamp}] [${level.toUpperCase()}]: ${message}`,
    ),
  ),
  transports: [new winston.transports.Console()],
});

const ALLOWED_TABLES = new Set(
  (process.env.ALLOWED_TABLES || "users,orders,products,inventory")
    .split(",")
    .map((t) => t.trim()),
);

const tablePkCache = new Map();

/**
 * Dynamically discovers primary key columns for a table.
 */
async function getTablePkColumns(node, tableName) {
  const cacheKey = `${node.id}:${tableName}`;
  if (tablePkCache.has(cacheKey)) return tablePkCache.get(cacheKey);

  const db = node.db;
  const client = node.client;
  let pkColumns = [];

  const columnInfo = await db(tableName).columnInfo();
  const columns = Object.keys(columnInfo);

  if (columns.includes("record_id")) {
    pkColumns = ["record_id"];
  } else if (columns.includes("id")) {
    pkColumns = ["id"];
  } else {
    if (client === "mysql2" || client === "mysql") {
      const pkQuery = await db.raw(
        `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'`,
        [tableName],
      );
      pkColumns = (pkQuery[0] || []).map((row) => row.COLUMN_NAME);
    } else if (client === "pg" || client === "postgres") {
      const pkQuery = await db.raw(
        `SELECT a.attname 
         FROM pg_index i 
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) 
         WHERE i.indrelid = ?::regclass AND i.indisprimary`,
        [tableName],
      );
      pkColumns = (pkQuery.rows || []).map((row) => row.attname);
    }
  }

  if (pkColumns.length === 0 && columns.length > 0) {
    pkColumns = [columns[0]];
  }

  tablePkCache.set(cacheKey, pkColumns);
  return pkColumns;
}

/**
 * Creates or updates change-data triggers for all configured allowed tables.
 */
async function setupNodeTriggers(node) {
  const db = node.db;
  const client = node.client;

  for (const tableName of ALLOWED_TABLES) {
    try {
      const hasTable = await db.schema.hasTable(tableName);
      if (!hasTable) continue;

      const columnInfo = await db(tableName).columnInfo();
      const columns = Object.keys(columnInfo);
      if (columns.length === 0) continue;

      const pkColumns = await getTablePkColumns(node, tableName);

      if (client === "mysql2" || client === "mysql") {
        const pkNewSql = `CONCAT_WS('-', ${pkColumns.map((c) => `NEW.\`${c}\``).join(", ")})`;
        const pkOldSql = `CONCAT_WS('-', ${pkColumns.map((c) => `OLD.\`${c}\``).join(", ")})`;

        const jsonFieldsNew = columns
          .map((col) => `'${col}', NEW.\`${col}\``)
          .join(", ");
        const jsonFieldsOld = columns
          .map((col) => `'${col}', OLD.\`${col}\``)
          .join(", ");

        await db.raw(`DROP TRIGGER IF EXISTS \`trg_${tableName}_ai\`;`);
        await db.raw(`DROP TRIGGER IF EXISTS \`trg_${tableName}_au\`;`);
        await db.raw(`DROP TRIGGER IF EXISTS \`trg_${tableName}_ad\`;`);

        await db.raw(`
          CREATE TRIGGER \`trg_${tableName}_ai\` AFTER INSERT ON \`${tableName}\` FOR EACH ROW
          BEGIN
            IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
              INSERT INTO \`sym_change_log\` (table_name, operation, primary_key_val, row_data, node_source_id, created_at)
              VALUES ('${tableName}', 'INSERT', ${pkNewSql}, JSON_OBJECT(${jsonFieldsNew}), '${node.id}', NOW());
            END IF;
          END;
        `);

        await db.raw(`
          CREATE TRIGGER \`trg_${tableName}_au\` AFTER UPDATE ON \`${tableName}\` FOR EACH ROW
          BEGIN
            IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
              INSERT INTO \`sym_change_log\` (table_name, operation, primary_key_val, row_data, node_source_id, created_at)
              VALUES ('${tableName}', 'UPDATE', ${pkNewSql}, JSON_OBJECT(${jsonFieldsNew}), '${node.id}', NOW());
            END IF;
          END;
        `);

        await db.raw(`
          CREATE TRIGGER \`trg_${tableName}_ad\` AFTER DELETE ON \`${tableName}\` FOR EACH ROW
          BEGIN
            IF @sym_is_syncing IS NULL OR @sym_is_syncing = FALSE THEN
              INSERT INTO \`sym_change_log\` (table_name, operation, primary_key_val, row_data, node_source_id, created_at)
              VALUES ('${tableName}', 'DELETE', ${pkOldSql}, JSON_OBJECT(${jsonFieldsOld}), '${node.id}', NOW());
            END IF;
          END;
        `);
      } else if (client === "pg" || client === "postgres") {
        const pgPkNewSql = pkColumns.map((c) => `NEW.${c}`).join(" || '-' || ");
        const pgPkOldSql = pkColumns.map((c) => `OLD.${c}`).join(" || '-' || ");

        const pgTriggerFunction = `
          CREATE OR REPLACE FUNCTION trg_${tableName}_sync() RETURNS TRIGGER AS $$
          BEGIN
            IF current_setting('sym.is_syncing', true) IS DISTINCT FROM 'true' THEN
              IF (TG_OP = 'DELETE') THEN
                INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id, created_at)
                VALUES ('${tableName}', TG_OP, ${pgPkOldSql}, row_to_json(OLD)::text, '${node.id}', NOW());
              ELSE
                INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id, created_at)
                VALUES ('${tableName}', TG_OP, ${pgPkNewSql}, row_to_json(NEW)::text, '${node.id}', NOW());
              END IF;
            END IF;
            RETURN COALESCE(NEW, OLD);
          END;
          $$ LANGUAGE plpgsql;
        `;

        await db.raw(pgTriggerFunction);
        await db.raw(`
          DROP TRIGGER IF EXISTS trg_${tableName}_sync ON "${tableName}";
          CREATE TRIGGER trg_${tableName}_sync
          AFTER INSERT OR UPDATE OR DELETE ON "${tableName}"
          FOR EACH ROW EXECUTE FUNCTION trg_${tableName}_sync();
        `);
      }
    } catch (err) {
      logger.error(
        `[Trigger Error] Node [${node.id}] Table [${tableName}]: ${err.message}`,
      );
    }
  }
}

module.exports = {
  getTablePkColumns,
  setupNodeTriggers,
  ALLOWED_TABLES,
};
