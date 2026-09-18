require("dotenv").config();
const knex = require("knex");
const winston = require("winston");
const crypto = require("crypto");

const { getTablePkColumns, ALLOWED_TABLES } = require("./triggers");

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

let ALLOWED_COLUMNS = {};
try {
  ALLOWED_COLUMNS = JSON.parse(process.env.ALLOWED_COLUMNS_JSON || "{}");
} catch (err) {
  logger.error(`Invalid ALLOWED_COLUMNS_JSON, ignoring: ${err.message}`);
}

function parseConnectionString(urlStr) {
  if (!urlStr)
    throw new Error("parseConnectionString: empty connection string");
  let parsed;
  try {
    parsed = new URL(urlStr);
  } catch (err) {
    throw new Error(`Invalid connection string: ${err.message}`);
  }
  return {
    host: parsed.hostname || "127.0.0.1",
    port: parsed.port ? parseInt(parsed.port, 10) : 3306,
    user: parsed.username ? decodeURIComponent(parsed.username) : undefined,
    password: parsed.password ? decodeURIComponent(parsed.password) : undefined,
    database: parsed.pathname.replace("/", ""),
  };
}

function normalizeClient(client) {
  if (!client) return "mysql2";
  const c = client.toLowerCase().trim();
  return c === "mysql" ? "mysql2" : c;
}

const hubClient = normalizeClient(process.env.LOCAL_CLIENT);
const isHubPg = hubClient === "pg" || hubClient === "postgres";

const HUB_NODE = {
  id: process.env.LOCAL_ID || "NODE_MAIN_HUB",
  client: hubClient,
  db: knex({
    client: hubClient,
    connection: isHubPg
      ? {
          connectionString: process.env.LOCAL_URL,
          ssl:
            process.env.LOCAL_SSL === "true"
              ? { rejectUnauthorized: process.env.LOCAL_SSL_STRICT !== "false" }
              : false,
        }
      : parseConnectionString(process.env.LOCAL_URL),
    pool: { min: 1, max: 10 },
  }),
};

const BRANCHES_CONFIG = JSON.parse(process.env.BRANCHES_JSON || "[]");

const branchNodes = BRANCHES_CONFIG.map((b) => {
  const clientName = normalizeClient(b.client);
  const isPg = clientName === "pg" || clientName === "postgres";

  return {
    id: b.id,
    client: clientName,
    db: knex({
      client: clientName,
      connection: isPg
        ? {
            connectionString: b.url,
            ssl: { rejectUnauthorized: b.sslInsecure !== true },
          }
        : parseConnectionString(b.url),
      pool: { min: 0, max: 5 },
    }),
  };
});

class PayloadCipher {
  static decrypt(cipherText, keyHex) {
    if (!keyHex || !cipherText) return cipherText;

    let parsed;
    try {
      parsed =
        typeof cipherText === "string" ? JSON.parse(cipherText) : cipherText;
    } catch {
      return cipherText;
    }

    if (
      !parsed ||
      typeof parsed !== "object" ||
      !parsed.iv ||
      !parsed.encrypted ||
      !parsed.authTag
    ) {
      return parsed;
    }

    try {
      const key = Buffer.from(keyHex, "hex");
      const decipher = crypto.createDecipheriv(
        "aes-256-gcm",
        key,
        Buffer.from(parsed.iv, "hex"),
      );
      decipher.setAuthTag(Buffer.from(parsed.authTag, "hex"));
      let decrypted = decipher.update(parsed.encrypted, "hex", "utf8");
      decrypted += decipher.final("utf8");
      return decrypted;
    } catch (err) {
      logger.error(`[Cipher Error] Decryption failed: ${err.message}`);
      throw new Error("Payload Decryption Failed");
    }
  }
}

const PK_CACHE = new Map();

async function getCachedPkColumns(node, tableName) {
  const cacheKey = `${node.id}:${tableName}`;
  if (!PK_CACHE.has(cacheKey)) {
    const cols = await getTablePkColumns(node, tableName);
    PK_CACHE.set(cacheKey, cols);
  }
  return PK_CACHE.get(cacheKey);
}

function decodePayload(change) {
  let rawPayloadJson = change.row_data;
  if (process.env.PAYLOAD_ENCRYPTION_KEY) {
    rawPayloadJson = PayloadCipher.decrypt(
      change.row_data,
      process.env.PAYLOAD_ENCRYPTION_KEY,
    );
  }

  let payload;
  if (typeof rawPayloadJson === "string") {
    try {
      payload = JSON.parse(rawPayloadJson);
    } catch {
      payload = rawPayloadJson;
    }
  } else {
    payload = rawPayloadJson;
  }

  if (!payload || typeof payload !== "object") return payload;

  const allowed = ALLOWED_COLUMNS[change.table_name];
  const targetKeys = allowed || Object.keys(payload);
  const result = {};

  for (const key of targetKeys) {
    if (Object.prototype.hasOwnProperty.call(payload, key)) {
      let val = payload[key];
      if (
        typeof val === "string" &&
        (val.startsWith("0000-00-00") || val.includes("0000-00-00"))
      ) {
        val = null;
      }
      result[key] = val;
    }
  }

  return result;
}

async function setSessionBypass(targetNode, trx) {
  const client = targetNode.client;
  if (client === "pg" || client === "postgres") {
    await trx.raw("SET LOCAL sym.is_syncing = 'true'");
  } else if (client === "mysql2" || client === "mysql") {
    await trx.raw("SET @sym_is_syncing = TRUE;");
    await trx.raw("SET FOREIGN_KEY_CHECKS = 0;");
    await trx.raw(
      "SET SESSION sql_mode = REPLACE(REPLACE(@@sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '');",
    );
  }
}

async function applyTableChanges(targetNode, trx, tableName, tableChanges) {
  const pkColumns = await getCachedPkColumns(targetNode, tableName);
  const deletes = [];
  const upsertsMap = new Map();

  for (const change of tableChanges) {
    if (change.operation === "DELETE") {
      deletes.push(change);
    } else {
      const payload = decodePayload(change);
      const pkKey = pkColumns.map((col) => payload[col]).join("-");
      upsertsMap.set(pkKey, payload);
    }
  }

  const upserts = Array.from(upsertsMap.values());

  if (deletes.length > 0) {
    if (pkColumns.length === 1) {
      const pkCol = pkColumns[0];
      const idsToDelete = deletes.map((d) => d.primary_key_val);
      await trx(tableName).whereIn(pkCol, idsToDelete).del();
    } else {
      await Promise.all(
        deletes.map((change) => {
          const deleteQuery = trx(tableName);
          const pkVals = String(change.primary_key_val).split("-");
          pkColumns.forEach((col, idx) => deleteQuery.where(col, pkVals[idx]));
          return deleteQuery.del();
        }),
      );
    }
  }

  if (upserts.length > 0) {
    await trx(tableName).insert(upserts).onConflict(pkColumns).merge();
  }
}

/**
 * Normalizes values across database engines and JSON payloads for precise equality checks.
 */
function normalizeVal(val) {
  if (val === null || val === undefined) return "";

  if (val instanceof Date) {
    return Math.floor(val.getTime() / 1000).toString();
  }

  if (typeof val === "string") {
    const dateParsed = Date.parse(val);
    if (!isNaN(dateParsed) && (val.includes("-") || val.includes("T"))) {
      return Math.floor(dateParsed / 1000).toString();
    }
    return val.trim();
  }

  if (typeof val === "boolean") {
    return val ? "1" : "0";
  }

  return String(val);
}

async function filterUnsyncedRecords(targetNode, tableName, tableChanges) {
  const pkColumns = await getCachedPkColumns(targetNode, tableName);
  if (!pkColumns || pkColumns.length === 0) return tableChanges;

  const unsynced = [];

  for (const change of tableChanges) {
    if (change.operation === "DELETE") {
      const pkVals = String(change.primary_key_val).split("-");
      const query = targetNode.db(tableName);
      pkColumns.forEach((col, idx) => query.where(col, pkVals[idx]));
      const rowExists = await query.first();
      if (rowExists) unsynced.push(change);
    } else {
      const payload = decodePayload(change);
      const query = targetNode.db(tableName);

      pkColumns.forEach((col) => {
        if (payload[col] !== undefined) {
          query.where(col, payload[col]);
        }
      });

      const targetRow = await query.first();

      if (!targetRow) {
        unsynced.push(change);
        continue;
      }

      let modified = false;
      for (const [col, val] of Object.entries(payload)) {
        if (targetRow[col] === undefined) continue;

        const normTarget = normalizeVal(targetRow[col]);
        const normSource = normalizeVal(val);

        if (normTarget !== normSource) {
          modified = true;
          break;
        }
      }

      if (modified) unsynced.push(change);
    }
  }

  return unsynced;
}

async function runRangeSync(
  sourceId,
  targetId,
  fromDateStr,
  toDateStr,
  checkOnly = false,
) {
  const allNodes = [HUB_NODE, ...branchNodes];
  const sourceNode = allNodes.find((n) => n.id === sourceId);
  const targetNode = allNodes.find((n) => n.id === targetId);

  if (!sourceNode || !targetNode) {
    const available = allNodes.map((n) => n.id).join(", ");
    logger.error(
      `Node mismatch: source ("${sourceId}") or target ("${targetId}") not found. Available nodes: [ ${available} ]`,
    );
    process.exit(1);
  }

  const fromDate = new Date(fromDateStr);
  const toDate = new Date(toDateStr);

  if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
    logger.error(
      "Invalid dates provided. Use format YYYY-MM-DDTHH:mm:ssZ or YYYY-MM-DD HH:mm:ss",
    );
    process.exit(1);
  }

  const modeText = checkOnly ? "[DRY RUN / CHECK ONLY]" : "[LIVE SYNC]";
  logger.info(
    `[Range Sync] ${modeText} Starting operation: ${sourceNode.id} -> ${targetNode.id} | Between ${fromDate.toISOString()} and ${toDate.toISOString()}`,
  );

  const batchSize = parseInt(process.env.BATCH_SIZE || "500", 10);
  let lastProcessedId = 0n;
  let totalRecordsFound = 0;
  const tableCounts = {};

  try {
    while (true) {
      const changes = await sourceNode
        .db("sym_change_log")
        .select(
          "change_id",
          "table_name",
          "operation",
          "primary_key_val",
          "row_data",
          "node_source_id",
        )
        .whereBetween("created_at", [fromDate, toDate])
        .where("change_id", ">", lastProcessedId.toString())
        .whereNot("node_source_id", targetNode.id)
        .orderBy("change_id", "asc")
        .limit(batchSize);

      if (changes.length === 0) break;

      const byTable = new Map();
      let validEvents = 0;

      for (const change of changes) {
        if (!ALLOWED_TABLES.has(change.table_name)) continue;
        if (!byTable.has(change.table_name)) byTable.set(change.table_name, []);
        byTable.get(change.table_name).push(change);
        validEvents++;
      }

      if (validEvents > 0) {
        if (!checkOnly) {
          await targetNode.db.transaction(async (trx) => {
            await setSessionBypass(targetNode, trx);

            for (const [tableName, tableChanges] of byTable) {
              await applyTableChanges(targetNode, trx, tableName, tableChanges);
              tableCounts[tableName] =
                (tableCounts[tableName] || 0) + tableChanges.length;
            }

            if (
              targetNode.client === "mysql2" ||
              targetNode.client === "mysql"
            ) {
              await trx.raw("SET FOREIGN_KEY_CHECKS = 1;");
            }
          });
          totalRecordsFound += validEvents;
          logger.info(
            `[Range Sync] Synced batch of ${validEvents} records (Total: ${totalRecordsFound})`,
          );
        } else {
          let batchUnsyncedCount = 0;
          for (const [tableName, tableChanges] of byTable) {
            const unsynced = await filterUnsyncedRecords(
              targetNode,
              tableName,
              tableChanges,
            );
            if (unsynced.length > 0) {
              tableCounts[tableName] =
                (tableCounts[tableName] || 0) + unsynced.length;
              batchUnsyncedCount += unsynced.length;
            }
          }
          totalRecordsFound += batchUnsyncedCount;
          logger.info(
            `[Check Only] Batch scanned: ${batchUnsyncedCount} unsynced/different records found.`,
          );
        }
      }

      lastProcessedId = BigInt(changes[changes.length - 1].change_id);
      if (changes.length < batchSize) break;
    }

    console.log("\n===========================================");
    console.log(` SUMMARY REPORT (${modeText})`);
    console.log("===========================================");
    console.log(`Source Node: ${sourceNode.id}`);
    console.log(`Target Node: ${targetNode.id}`);
    console.log(
      `Date Range : ${fromDate.toISOString()} to ${toDate.toISOString()}`,
    );
    console.log(`Total Unsynced Events Found: ${totalRecordsFound}`);
    console.log("-------------------------------------------");
    console.log("Breakdown by Table:");

    if (Object.keys(tableCounts).length === 0) {
      console.log("  No unsynced records found.");
    } else {
      for (const [table, count] of Object.entries(tableCounts)) {
        console.log(`  - ${table}: ${count} records`);
      }
    }
    console.log("===========================================\n");
  } finally {
    await HUB_NODE.db.destroy();
    await Promise.all(branchNodes.map((b) => b.db.destroy()));
  }

  process.exit(0);
}

const rawArgs = process.argv.slice(2);
const checkOnly = rawArgs.includes("--check-only");
const args = rawArgs.filter((arg) => arg !== "--check-only");

if (args.length < 4) {
  console.log(`
Usage:
  node range-sync.js <SOURCE_NODE_ID> <TARGET_NODE_ID> <FROM_DATE> <TO_DATE> [--check-only]

Examples:
  # Dry run / inspection only:
  node range-sync.js NODE_BRANCH_02 NODE_MAIN_HUB "2026-09-01 00:00:00" "2026-09-18 23:59:59" --check-only

  # Live sync execution:
  node range-sync.js NODE_BRANCH_02 NODE_MAIN_HUB "2026-09-01 00:00:00" "2026-09-18 23:59:59"
  `);
  process.exit(1);
}

runRangeSync(args[0], args[1], args[2], args[3], checkOnly);
