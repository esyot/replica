require("dotenv").config();
const knex = require("knex");
const winston = require("winston");
const crypto = require("crypto");

const { getTablePkColumns, ALLOWED_TABLES } = require("./triggers");
const { decodeCompositeKey } = require("./pk-codec");

/* ------------------------------------------------------------------ */
/* Logging                                                            */
/* ------------------------------------------------------------------ */

const logger = winston.createLogger({
  level: process.env.NODE_ENV === "production" ? "info" : "debug",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.printf(
      ({ timestamp, level, message, stack }) =>
        `[${timestamp}] [pid:${process.pid}] [${level.toUpperCase()}]: ${stack || message}`,
    ),
  ),
  transports: [new winston.transports.Console()],
});

/* ------------------------------------------------------------------ */
/* Env helpers                                                        */
/* ------------------------------------------------------------------ */

function parseIntEnv(name, defaultValue, { min, max } = {}) {
  const raw = process.env[name];
  let val = raw !== undefined && raw !== "" ? parseInt(raw, 10) : defaultValue;
  if (Number.isNaN(val)) {
    if (raw !== undefined && raw !== "") {
      logger.warn(
        `Invalid integer for ${name}="${raw}", using default ${defaultValue}`,
      );
    }
    val = defaultValue;
  }
  if (min !== undefined) val = Math.max(min, val);
  if (max !== undefined) val = Math.min(max, val);
  return val;
}

let ALLOWED_COLUMNS = {};
try {
  ALLOWED_COLUMNS = JSON.parse(process.env.ALLOWED_COLUMNS_JSON || "{}");
} catch (err) {
  logger.error(`Invalid ALLOWED_COLUMNS_JSON, ignoring: ${err.message}`);
}

/* ------------------------------------------------------------------ */
/* Connection helpers                                                 */
/* ------------------------------------------------------------------ */

function parseConnectionString(urlStr, defaultPort = 3306) {
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
    port: parsed.port ? parseInt(parsed.port, 10) : defaultPort,
    user: parsed.username ? decodeURIComponent(parsed.username) : undefined,
    password: parsed.password ? decodeURIComponent(parsed.password) : undefined,
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

function isPgClient(client) {
  return client === "pg" || client === "postgres";
}

function isMysqlClient(client) {
  return client === "mysql2" || client === "mysql";
}

function defaultPortFor(client) {
  return isPgClient(client) ? 5432 : 3306;
}

function buildKnexConnection(clientName, { url, ssl, sslStrict }) {
  if (isPgClient(clientName)) {
    return {
      connectionString: url,
      ssl: ssl ? { rejectUnauthorized: sslStrict !== false } : false,
    };
  }
  return parseConnectionString(url, defaultPortFor(clientName));
}

/* ------------------------------------------------------------------ */
/* Encryption key validation                                          */
/* ------------------------------------------------------------------ */

if (process.env.PAYLOAD_ENCRYPTION_KEY) {
  let keyBuf;
  try {
    keyBuf = Buffer.from(process.env.PAYLOAD_ENCRYPTION_KEY, "hex");
  } catch {
    keyBuf = Buffer.alloc(0);
  }
  if (keyBuf.length !== 32) {
    logger.error(
      "CRITICAL: PAYLOAD_ENCRYPTION_KEY must be 32 bytes (64 hex chars) for aes-256-gcm.",
    );
    process.exit(1);
  }
}

/* ------------------------------------------------------------------ */
/* Node definitions                                                   */
/* ------------------------------------------------------------------ */

const hubClient = normalizeClient(process.env.LOCAL_CLIENT);

const HUB_NODE = {
  id: process.env.LOCAL_ID || "NODE_MAIN_HUB",
  client: hubClient,
  db: knex({
    client: hubClient,
    connection: buildKnexConnection(hubClient, {
      url: process.env.LOCAL_URL,
      ssl: process.env.LOCAL_SSL === "true",
      sslStrict: process.env.LOCAL_SSL_STRICT !== "false",
    }),
    pool: { min: 1, max: 10 },
  }),
};

let BRANCHES_CONFIG = [];
try {
  BRANCHES_CONFIG = JSON.parse(process.env.BRANCHES_JSON || "[]");
  if (!Array.isArray(BRANCHES_CONFIG)) {
    throw new Error("BRANCHES_JSON must be a JSON array");
  }
} catch (err) {
  logger.error(`CRITICAL: Invalid BRANCHES_JSON: ${err.message}`);
  process.exit(1);
}

for (const b of BRANCHES_CONFIG) {
  if (!b.id || !b.url) {
    logger.error(
      `CRITICAL: Each entry in BRANCHES_JSON needs "id" and "url". Got: ${JSON.stringify(b)}`,
    );
    process.exit(1);
  }
}

const branchNodes = BRANCHES_CONFIG.map((b) => {
  const clientName = normalizeClient(b.client);
  return {
    id: b.id,
    client: clientName,
    db: knex({
      client: clientName,
      connection: buildKnexConnection(clientName, {
        url: b.url,
        ssl: true,
        sslStrict: b.sslInsecure !== true,
      }),
      pool: { min: 0, max: 5 },
    }),
  };
});

/* ------------------------------------------------------------------ */
/* Payload cipher                                                     */
/* ------------------------------------------------------------------ */

class PayloadCipher {
  static decrypt(cipherText, keyHex) {
    if (!keyHex || cipherText == null) return cipherText;

    let parsed;
    try {
      parsed =
        typeof cipherText === "string" ? JSON.parse(cipherText) : cipherText;
    } catch {
      // Not valid JSON. If encryption is on, this is unexpected — fail loud.
      throw new Error("Payload is not a valid encrypted envelope");
    }

    if (
      !parsed ||
      typeof parsed !== "object" ||
      !parsed.iv ||
      !parsed.encrypted ||
      !parsed.authTag
    ) {
      throw new Error("Payload is plaintext but PAYLOAD_ENCRYPTION_KEY is set");
    }

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
  }
}

/* ------------------------------------------------------------------ */
/* PK cache (promise-cached, failure-safe)                            */
/* ------------------------------------------------------------------ */

const PK_CACHE = new Map();

async function getCachedPkColumns(node, tableName) {
  if (!ALLOWED_TABLES.has(tableName)) {
    throw new Error(
      `Refusing to resolve PK columns for disallowed table: ${tableName}`,
    );
  }
  const cacheKey = `${node.id}:${tableName}`;
  if (!PK_CACHE.has(cacheKey)) {
    const promise = getTablePkColumns(node, tableName).then((cols) => {
      if (!cols || cols.length === 0) {
        throw new Error(
          `No primary key columns resolved for table: ${tableName}`,
        );
      }
      return cols;
    });
    promise.catch(() => PK_CACHE.delete(cacheKey));
    PK_CACHE.set(cacheKey, promise);
  }
  return PK_CACHE.get(cacheKey);
}

/* ------------------------------------------------------------------ */
/* Payload decoding                                                   */
/* ------------------------------------------------------------------ */

const ZERO_DATE_RE = /^0000-00-00(?: 00:00:00)?$/;

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
  const targetKeys = Array.isArray(allowed) ? allowed : Object.keys(payload);
  const result = {};

  for (const key of targetKeys) {
    if (Object.prototype.hasOwnProperty.call(payload, key)) {
      let val = payload[key];
      if (typeof val === "string" && ZERO_DATE_RE.test(val)) val = null;
      result[key] = val;
    }
  }

  return result;
}

/* ------------------------------------------------------------------ */
/* Session bypass (with reliable restore)                             */
/* ------------------------------------------------------------------ */

async function setSessionBypass(targetNode, trx) {
  const client = targetNode.client;
  if (isPgClient(client)) {
    await trx.raw("SET LOCAL sym.is_syncing = 'true'");
    return async () => {}; // SET LOCAL is transaction-scoped
  }
  if (isMysqlClient(client)) {
    await trx.raw("SET @sym_is_syncing = TRUE;");
    await trx.raw("SET FOREIGN_KEY_CHECKS = 0;");
    await trx.raw(
      "SET SESSION sql_mode = REPLACE(REPLACE(@@sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '');",
    );
    return async () => {
      try {
        await trx.raw("SET FOREIGN_KEY_CHECKS = 1;");
      } catch (err) {
        logger.error(
          `[SessionBypass] Failed to restore FOREIGN_KEY_CHECKS on ${targetNode.id}: ${err.message}`,
        );
      }
    };
  }
  return async () => {};
}

/* ------------------------------------------------------------------ */
/* Apply changes (ordered: per-table upserts and deletes by change_id)*/
/* ------------------------------------------------------------------ */

async function applyTableChanges(targetNode, trx, tableName, tableChanges) {
  const pkColumns = await getCachedPkColumns(targetNode, tableName);
  const pkCount = pkColumns.length;

  // Resolve payloads once, in change_id order (callers already pass sorted).
  const resolved = [];
  for (const change of tableChanges) {
    if (change.operation === "DELETE") {
      resolved.push({ kind: "delete", change });
    } else {
      const payload = decodePayload(change);
      const missing = pkColumns.filter((c) => payload[c] === undefined);
      if (missing.length > 0) {
        logger.warn(
          `[Apply] ${tableName} | change_id ${change.change_id}: payload missing PK column(s) ` +
            `[${missing.join(", ")}] (likely filtered by ALLOWED_COLUMNS_JSON). This row may collide or fail.`,
        );
      }
      resolved.push({ kind: "upsert", change, payload });
    }
  }

  // Collapse consecutive same-kind operations, preserving final state per PK.
  // Walk in change_id order: last write wins. This correctly handles
  // INSERT(pk=1) followed by DELETE(pk=1) in the same batch.
  const finalDeletes = [];
  const finalUpserts = new Map(); // pkKey -> payload (last write wins)

  for (const item of resolved) {
    if (item.kind === "delete") {
      const pkVals =
        pkCount === 1
          ? [item.change.primary_key_val]
          : decodeCompositeKey(item.change.primary_key_val, pkCount, tableName);
      const pkKey = JSON.stringify(pkVals);
      finalUpserts.delete(pkKey);
      finalDeletes.push(item.change);
    } else {
      const pkKey = JSON.stringify(
        pkColumns.map((col) => item.payload[col] ?? null),
      );
      finalUpserts.set(pkKey, item.payload);
    }
  }

  // Apply deletes in chunks.
  if (finalDeletes.length > 0) {
    if (pkCount === 1) {
      const pkCol = pkColumns[0];
      const ids = finalDeletes.map((d) => d.primary_key_val);
      const chunkSize = parseIntEnv("DELETE_CHUNK_SIZE", 500, {
        min: 1,
        max: 5000,
      });
      for (let i = 0; i < ids.length; i += chunkSize) {
        await trx(tableName)
          .whereIn(pkCol, ids.slice(i, i + chunkSize))
          .del();
      }
    } else {
      const chunkSize = parseIntEnv("COMPOSITE_DELETE_CHUNK_SIZE", 200, {
        min: 1,
        max: 1000,
      });
      for (let i = 0; i < finalDeletes.length; i += chunkSize) {
        const chunk = finalDeletes.slice(i, i + chunkSize);
        const builder = trx(tableName);
        builder.where(function () {
          for (const change of chunk) {
            const pkVals = decodeCompositeKey(
              change.primary_key_val,
              pkCount,
              tableName,
            );
            this.orWhere(function () {
              pkColumns.forEach((col, idx) => this.andWhere(col, pkVals[idx]));
            });
          }
        });
        await builder.del();
      }
    }
  }

  // Apply upserts in chunks (parameter-limit safe).
  const upserts = Array.from(finalUpserts.values());
  if (upserts.length > 0) {
    const chunkSize = parseIntEnv("UPSERT_CHUNK_SIZE", 200, {
      min: 1,
      max: 2000,
    });
    for (let i = 0; i < upserts.length; i += chunkSize) {
      await trx(tableName)
        .insert(upserts.slice(i, i + chunkSize))
        .onConflict(pkColumns)
        .merge();
    }
  }

  logger.debug(
    `[Apply] ${targetNode.id} | ${tableName}: ${upserts.length} upserted, ${finalDeletes.length} deleted`,
  );
}

/* ------------------------------------------------------------------ */
/* Value normalization (for check-only diff)                          */
/* ------------------------------------------------------------------ */

const ISO_DATE_PREFIX_RE = /^\d{4}-\d{2}-\d{2}/;
const NUMERIC_RE = /^-?\d+(\.\d+)?$/;

function normalizeVal(val) {
  if (val === null || val === undefined || val === "") return "";

  if (val instanceof Date) return Math.floor(val.getTime() / 1000).toString();

  if (typeof val === "boolean") return val ? "1" : "0";
  if (val === "true" || val === "TRUE") return "1";
  if (val === "false" || val === "FALSE") return "0";

  if (typeof val === "string") {
    const trimmed = val.trim();

    if (ISO_DATE_PREFIX_RE.test(trimmed)) {
      const dateStr = trimmed.includes("T")
        ? trimmed
        : trimmed.replace(" ", "T") + "Z";
      const parsedTime = Date.parse(dateStr);
      if (!Number.isNaN(parsedTime))
        return Math.floor(parsedTime / 1000).toString();
    }

    if (NUMERIC_RE.test(trimmed)) return Number(trimmed).toString();
    return trimmed;
  }

  if (typeof val === "number") return val.toString();
  return String(val);
}

/* ------------------------------------------------------------------ */
/* Check-only: batched existence + diff                               */
/* ------------------------------------------------------------------ */

async function filterUnsyncedRecords(targetNode, tableName, tableChanges) {
  const pkColumns = await getCachedPkColumns(targetNode, tableName);
  if (!pkColumns || pkColumns.length === 0) return tableChanges;
  const pkCount = pkColumns.length;

  // Separate ops and collect PK tuples.
  const deleteChanges = [];
  const upsertChanges = [];
  const unresolved = []; // missing PK cols → mark unsynced for review

  for (const change of tableChanges) {
    if (change.operation === "DELETE") {
      deleteChanges.push(change);
    } else {
      const payload = decodePayload(change);
      const missing = pkColumns.filter((c) => payload[c] === undefined);
      if (missing.length > 0) {
        logger.warn(
          `[Check Only] ${tableName} | change_id ${change.change_id}: payload missing PK column(s) ` +
            `[${missing.join(", ")}] (likely filtered by ALLOWED_COLUMNS_JSON) — cannot safely check existence, marking as unsynced for manual review.`,
        );
        unresolved.push(change);
        continue;
      }
      upsertChanges.push({ change, payload });
    }
  }

  // --- Existence check for DELETEs (batched) ---
  const deletesStillPresent = [];
  if (deleteChanges.length > 0) {
    const pkTuples = deleteChanges.map((c) =>
      pkCount === 1
        ? [c.primary_key_val]
        : decodeCompositeKey(c.primary_key_val, pkCount, tableName),
    );

    const foundRows = new Set();
    const chunkSize = parseIntEnv("CHECK_CHUNK_SIZE", 500, {
      min: 1,
      max: 5000,
    });

    for (let i = 0; i < pkTuples.length; i += chunkSize) {
      const chunk = pkTuples.slice(i, i + chunkSize);
      const query = targetNode.db(tableName).select(pkColumns);
      query.where(function () {
        for (const tuple of chunk) {
          this.orWhere(function () {
            pkColumns.forEach((col, idx) => this.andWhere(col, tuple[idx]));
          });
        }
      });
      const rows = await query;
      for (const row of rows) {
        foundRows.add(
          JSON.stringify(pkColumns.map((c) => normalizeVal(row[c]))),
        );
      }
    }

    deleteChanges.forEach((change, idx) => {
      const key = JSON.stringify(pkTuples[idx].map((v) => normalizeVal(v)));
      if (foundRows.has(key)) deletesStillPresent.push(change);
    });
  }

  // --- Diff check for UPSERTs (batched fetch + in-memory compare) ---
  const upsertsStillDiff = [];
  if (upsertChanges.length > 0) {
    const targetRows = new Map();
    const chunkSize = parseIntEnv("CHECK_CHUNK_SIZE", 500, {
      min: 1,
      max: 5000,
    });

    for (let i = 0; i < upsertChanges.length; i += chunkSize) {
      const chunk = upsertChanges.slice(i, i + chunkSize);
      const query = targetNode.db(tableName);
      query.where(function () {
        for (const { payload } of chunk) {
          this.orWhere(function () {
            pkColumns.forEach((col) => this.andWhere(col, payload[col]));
          });
        }
      });
      const rows = await query;
      for (const row of rows) {
        const key = JSON.stringify(pkColumns.map((c) => normalizeVal(row[c])));
        targetRows.set(key, row);
      }
    }

    for (const { change, payload } of upsertChanges) {
      const key = JSON.stringify(
        pkColumns.map((c) => normalizeVal(payload[c])),
      );
      const targetRow = targetRows.get(key);

      if (!targetRow) {
        upsertsStillDiff.push(change);
        continue;
      }

      let modified = false;
      for (const [col, val] of Object.entries(payload)) {
        if (targetRow[col] === undefined) continue;
        const normTarget = normalizeVal(targetRow[col]);
        const normSource = normalizeVal(val);
        if (normTarget !== normSource) {
          logger.info(
            `[MISMATCH] ${tableName} | col: "${col}" | TargetDB: "${normTarget}" (raw: ${JSON.stringify(targetRow[col])}) vs Payload: "${normSource}" (raw: ${JSON.stringify(val)})`,
          );
          modified = true;
          break;
        }
      }
      if (modified) upsertsStillDiff.push(change);
    }
  }

  return [...deletesStillPresent, ...upsertsStillDiff, ...unresolved];
}

/* ------------------------------------------------------------------ */
/* Date parsing (explicit, timezone-stable)                           */
/* ------------------------------------------------------------------ */

function parseBoundaryDate(input, label) {
  if (!input) throw new Error(`${label} is required`);
  let s = String(input).trim();
  // If no timezone info, treat as UTC so behavior is machine-independent.
  if (!/[zZ]$|[+-]\d{2}:?\d{2}$/.test(s)) {
    if (s.includes("T")) s = `${s}Z`;
    else s = `${s.replace(" ", "T")}Z`;
  }
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) {
    throw new Error(
      `Invalid ${label} "${input}". Use ISO 8601 with offset, e.g. "2026-09-01T00:00:00Z" or "2026-09-01 00:00:00".`,
    );
  }
  return d;
}

/* ------------------------------------------------------------------ */
/* Range sync                                                         */
/* ------------------------------------------------------------------ */

async function runRangeSync(
  sourceId,
  targetId,
  fromDateStr,
  toDateStr,
  checkOnly,
) {
  const allNodes = [HUB_NODE, ...branchNodes];
  const sourceNode = allNodes.find((n) => n.id === sourceId);
  const targetNode = allNodes.find((n) => n.id === targetId);

  if (!sourceNode || !targetNode) {
    const available = allNodes.map((n) => n.id).join(", ");
    logger.error(
      `Node mismatch: source ("${sourceId}") or target ("${targetId}") not found. Available nodes: [ ${available} ]`,
    );
    return 1;
  }

  let fromDate, toDate;
  try {
    fromDate = parseBoundaryDate(fromDateStr, "FROM date");
    toDate = parseBoundaryDate(toDateStr, "TO date");
  } catch (err) {
    logger.error(err.message);
    return 1;
  }

  if (fromDate.getTime() > toDate.getTime()) {
    logger.error(
      `Invalid range: FROM (${fromDate.toISOString()}) is after TO (${toDate.toISOString()}).`,
    );
    return 1;
  }

  // Treat upper bound as exclusive by shifting to next millisecond and using <.
  // This closes the sub-second tail hole when users pass "23:59:59".
  const upperExclusive = new Date(toDate.getTime() + 1);

  const modeText = checkOnly
    ? "[DRY RUN / CHECK ONLY]"
    : "[LIVE SYNC / COMMIT]";
  logger.info(
    `[Range Sync] ${modeText} ${sourceNode.id} -> ${targetNode.id} | ${fromDate.toISOString()} <= created_at < ${upperExclusive.toISOString()}`,
  );

  const batchSize = parseIntEnv("BATCH_SIZE", 500, { min: 1, max: 5000 });
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
        .where("created_at", ">=", fromDate)
        .where("created_at", "<", upperExclusive)
        .where("change_id", ">", lastProcessedId.toString())
        .whereNot("node_source_id", targetNode.id)
        .orderBy("change_id", "asc")
        .limit(batchSize);

      if (changes.length === 0) break;

      const byTable = new Map();
      let validEvents = 0;
      let skippedEvents = 0;

      for (const change of changes) {
        if (!ALLOWED_TABLES.has(change.table_name)) {
          skippedEvents++;
          continue;
        }
        if (!byTable.has(change.table_name)) byTable.set(change.table_name, []);
        byTable.get(change.table_name).push(change);
        validEvents++;
      }

      if (skippedEvents > 0) {
        logger.warn(
          `[Range Sync] Skipped ${skippedEvents} events for disallowed tables`,
        );
      }

      if (validEvents > 0) {
        if (!checkOnly) {
          let restoreSession = async () => {};
          try {
            await targetNode.db.transaction(async (trx) => {
              restoreSession = await setSessionBypass(targetNode, trx);
              for (const [tableName, tableChanges] of byTable) {
                await applyTableChanges(
                  targetNode,
                  trx,
                  tableName,
                  tableChanges,
                );
                tableCounts[tableName] =
                  (tableCounts[tableName] || 0) + tableChanges.length;
              }
            });
          } finally {
            await restoreSession();
          }
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

      await new Promise((r) => setImmediate(r));
    }

    const label = checkOnly ? "Unsynced Events Found" : "Events Synced";

    console.log("\n===========================================");
    console.log(` SUMMARY REPORT (${modeText})`);
    console.log("===========================================");
    console.log(`Source Node: ${sourceNode.id}`);
    console.log(`Target Node: ${targetNode.id}`);
    console.log(
      `Date Range : ${fromDate.toISOString()} <= created_at < ${upperExclusive.toISOString()}`,
    );
    console.log(`Total ${label}: ${totalRecordsFound}`);
    console.log("-------------------------------------------");
    console.log("Breakdown by Table:");
    if (Object.keys(tableCounts).length === 0) {
      console.log("  No records.");
    } else {
      for (const [table, count] of Object.entries(tableCounts)) {
        console.log(`  - ${table}: ${count} records`);
      }
    }
    console.log("===========================================\n");

    return 0;
  } catch (err) {
    logger.error(`[Range Sync] Fatal error: ${err.stack || err.message}`);
    return 2;
  }
}

/* ------------------------------------------------------------------ */
/* CLI                                                                */
/* ------------------------------------------------------------------ */

const HELP = `
Usage:
  node range-sync.js <SOURCE_NODE_ID> <TARGET_NODE_ID> <FROM_DATE> <TO_DATE> [--commit]

Safety:
  By default the tool runs in CHECK-ONLY mode and performs no writes.
  Pass --commit to actually write to the target.

Dates:
  ISO 8601. Strings without an offset are interpreted as UTC.
  The upper bound is treated as exclusive (< TO + 1ms), so "23:59:59"
  correctly includes sub-second data at that second.

Examples:
  # Inspect only (no writes):
  node range-sync.js NODE_BRANCH_02 NODE_MAIN_HUB "2026-09-01T00:00:00Z" "2026-09-19T00:00:00Z"

  # Actually sync:
  node range-sync.js NODE_BRANCH_02 NODE_MAIN_HUB "2026-09-01T00:00:00Z" "2026-09-19T00:00:00Z" --commit
`;

function parseCli(argv) {
  const knownFlags = new Set(["--commit", "--check-only", "--help", "-h"]);
  const unknown = argv.filter((a) => a.startsWith("-") && !knownFlags.has(a));
  if (unknown.length > 0) {
    console.error(`Unknown flag(s): ${unknown.join(", ")}\n`);
    console.error(HELP);
    return { error: true };
  }

  if (argv.includes("--help") || argv.includes("-h")) {
    console.log(HELP);
    return { help: true };
  }

  const positional = argv.filter((a) => !a.startsWith("-"));
  if (positional.length < 4) {
    console.error(HELP);
    return { error: true };
  }

  const commit = argv.includes("--commit");
  // --check-only is accepted for backward compat; default is already check-only.
  const checkOnly = !commit;

  return {
    sourceId: positional[0],
    targetId: positional[1],
    fromDate: positional[2],
    toDate: positional[3],
    checkOnly,
    commit,
  };
}

async function main() {
  const parsed = parseCli(process.argv.slice(2));

  if (parsed.help) return 0;
  if (parsed.error) return 1;

  if (!parsed.checkOnly) {
    logger.warn(
      "[Range Sync] LIVE COMMIT MODE — writes will be performed on the target. " +
        "Press Ctrl+C within 3 seconds to abort.",
    );
    await new Promise((r) => setTimeout(r, 3000));
  }

  try {
    const code = await runRangeSync(
      parsed.sourceId,
      parsed.targetId,
      parsed.fromDate,
      parsed.toDate,
      parsed.checkOnly,
    );
    return code;
  } finally {
    try {
      await HUB_NODE.db.destroy();
    } catch (err) {
      logger.error(`Error closing hub DB: ${err.message}`);
    }
    const results = await Promise.allSettled(
      branchNodes.map((b) => b.db.destroy()),
    );
    results.forEach((r, i) => {
      if (r.status === "rejected") {
        logger.error(
          `Error closing branch [${branchNodes[i].id}]: ${r.reason?.message}`,
        );
      }
    });
  }
}

if (require.main === module) {
  main()
    .then((code) => {
      process.exitCode = code || 0;
    })
    .catch((err) => {
      logger.error(`[Fatal] ${err.stack || err.message}`);
      process.exitCode = 2;
    });
}

module.exports = { runRangeSync, filterUnsyncedRecords, applyTableChanges };
