#!/usr/bin/env node
/**
 * force-sync.js
 *
 * One-shot script that forces hub <-> branch sync until caught up.
 * Reuses the same conventions as server.js (env vars, BRANCHES_JSON, etc.)
 *
 * Usage:
 *   node force-sync.js                 # sync all branches
 *   node force-sync.js NODE_BRANCH_1   # sync only specific branch(es)
 *
 * Exit codes:
 *   0 = fully synced (all checkpoints caught up to max source change_id)
 *   1 = error or did not converge within limits
 */

require("dotenv").config();
const knex = require("knex");
const winston = require("winston");
const crypto = require("crypto");

const {
  getTablePkColumns,
  setupNodeTriggers,
  ALLOWED_TABLES,
} = require("./triggers");
const { decodeCompositeKey } = require("./pk-codec");

/* ------------------------------------------------------------------ */
/* Logging                                                            */
/* ------------------------------------------------------------------ */

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

/* ------------------------------------------------------------------ */
/* Env helpers (mirrors server.js)                                    */
/* ------------------------------------------------------------------ */

function requireEnv(name) {
  const val = process.env[name];
  if (!val) {
    logger.error(`CRITICAL: ${name} missing in environment!`);
    process.exit(1);
  }
  return val;
}

function parseIntEnv(name, defaultValue, { min, max } = {}) {
  const raw = process.env[name];
  let val = raw !== undefined && raw !== "" ? parseInt(raw, 10) : defaultValue;
  if (Number.isNaN(val)) val = defaultValue;
  if (min !== undefined) val = Math.max(min, val);
  if (max !== undefined) val = Math.min(max, val);
  return val;
}

function parseBoolEnv(name, defaultValue = false) {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return defaultValue;
  return raw === "true" || raw === "1";
}

/* ------------------------------------------------------------------ */
/* Column allowlist (needed by SyncWorker.decodePayload)              */
/* ------------------------------------------------------------------ */

let ALLOWED_COLUMNS = {};
try {
  ALLOWED_COLUMNS = JSON.parse(process.env.ALLOWED_COLUMNS_JSON || "{}");
} catch (err) {
  logger.error(`Invalid ALLOWED_COLUMNS_JSON, ignoring: ${err.message}`);
}

/* ------------------------------------------------------------------ */
/* Connection helpers (mirrors server.js)                             */
/* ------------------------------------------------------------------ */

function parseConnectionString(urlStr, defaultPort = 3306) {
  const parsed = new URL(urlStr);
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

function buildKnexConnection(nodeClient, opts) {
  if (isPgClient(nodeClient)) {
    return {
      connectionString: opts.url,
      ssl: opts.ssl ? { rejectUnauthorized: opts.sslStrict !== false } : false,
    };
  }
  return parseConnectionString(opts.url, defaultPortFor(nodeClient));
}

/* ------------------------------------------------------------------ */
/* Payload cipher (mirrors server.js)                                 */
/* ------------------------------------------------------------------ */

class PayloadCipher {
  static decrypt(cipherText, keyHex) {
    if (!keyHex || cipherText == null) return cipherText;
    const parsed =
      typeof cipherText === "string" ? JSON.parse(cipherText) : cipherText;
    if (!parsed || !parsed.iv || !parsed.encrypted || !parsed.authTag) {
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
/* Config                                                             */
/* ------------------------------------------------------------------ */

const CONFIG = {
  batchSize: parseIntEnv("BATCH_SIZE", 500, { min: 1, max: 5000 }),
  upsertChunkSize: parseIntEnv("UPSERT_CHUNK_SIZE", 200, { min: 1, max: 2000 }),
  encryptionKey: process.env.PAYLOAD_ENCRYPTION_KEY || null,
  compositeDeleteChunkSize: parseIntEnv("COMPOSITE_DELETE_CHUNK_SIZE", 200, {
    min: 1,
    max: 1000,
  }),
  // Extra safety limit for this script (distinct from server's flush).
  forceMaxBatches: parseIntEnv("FORCE_SYNC_MAX_BATCHES", 100000, { min: 1 }),
};

/* ------------------------------------------------------------------ */
/* PK cache (mirrors server.js)                                       */
/* ------------------------------------------------------------------ */

const PK_CACHE = new Map();

async function getCachedPkColumns(node, tableName) {
  if (!ALLOWED_TABLES.has(tableName)) {
    throw new Error(`Refusing to resolve PK columns for: ${tableName}`);
  }
  const cacheKey = `${node.id}:${tableName}`;
  if (!PK_CACHE.has(cacheKey)) {
    const promise = getTablePkColumns(node, tableName).then((cols) => {
      if (!cols || cols.length === 0) {
        throw new Error(`No primary key columns resolved for: ${tableName}`);
      }
      return cols;
    });
    promise.catch(() => PK_CACHE.delete(cacheKey));
    PK_CACHE.set(cacheKey, promise);
  }
  return PK_CACHE.get(cacheKey);
}

/* ------------------------------------------------------------------ */
/* SyncWorker (mirrors server.js, without loop/backoff)               */
/* ------------------------------------------------------------------ */

class SyncWorker {
  constructor(source, target) {
    this.source = source;
    this.target = target;
    this.isProcessing = false;
    this.stats = {
      appliedEvents: 0,
      skippedEvents: 0,
      quarantinedEvents: 0,
      batches: 0,
      lastAppliedAt: null,
      lastQuarantineChangeId: null,
    };
  }

  async getCheckpoint() {
    const record = await this.target
      .db("sym_checkpoint")
      .where("node_id", this.source.id)
      .first();
    return record ? BigInt(record.last_processed_change_id) : 0n;
  }

  async setCheckpoint(lastChangeId, trx) {
    const db = trx || this.target.db;
    await db("sym_checkpoint")
      .insert({
        node_id: this.source.id,
        last_processed_change_id: lastChangeId.toString(),
        updated_at: new Date(),
      })
      .onConflict("node_id")
      .merge();
  }

  async getSourceMaxChangeId() {
    const row = await this.source
      .db("sym_change_log")
      .max("change_id as max_id")
      .first();
    return row?.max_id != null ? BigInt(row.max_id) : 0n;
  }

  async setSessionBypass(trx) {
    const client = this.target.client;
    if (isPgClient(client)) {
      await trx.raw("SET LOCAL sym.is_syncing = 'true'");
      return async () => {};
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
            `[SessionBypass] Failed to restore FOREIGN_KEY_CHECKS on ${this.target.id}: ${err.message}`,
          );
        }
      };
    }
    return async () => {};
  }

  decodePayload(change) {
    let rawPayloadJson = change.row_data;
    if (CONFIG.encryptionKey) {
      rawPayloadJson = PayloadCipher.decrypt(
        change.row_data,
        CONFIG.encryptionKey,
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
        if (typeof val === "string" && /^0000-00-00(?: 00:00:00)?$/.test(val)) {
          val = null;
        }
        result[key] = val;
      }
    }
    return result;
  }

  async bulkDeleteComposite(trx, tableName, pkColumns, deletes) {
    const chunkSize = CONFIG.compositeDeleteChunkSize;
    for (let i = 0; i < deletes.length; i += chunkSize) {
      const chunk = deletes.slice(i, i + chunkSize);
      const builder = trx(tableName);
      builder.where(function () {
        for (const change of chunk) {
          const pkVals = decodeCompositeKey(
            change.primary_key_val,
            pkColumns.length,
            tableName,
          );
          this.orWhere(function () {
            pkColumns.forEach((col, idx) => {
              this.andWhere(col, pkVals[idx]);
            });
          });
        }
      });
      await builder.del();
    }
  }

  async applyTableChanges(trx, tableName, tableChanges) {
    const pkColumns = await getCachedPkColumns(this.target, tableName);
    const deletes = [];
    const upserts = [];

    for (const change of tableChanges) {
      if (change.operation === "DELETE") {
        deletes.push(change);
      } else {
        upserts.push(this.decodePayload(change));
      }
    }

    if (deletes.length > 0) {
      if (pkColumns.length === 1) {
        const pkCol = pkColumns[0];
        const idsToDelete = deletes.map((d) => d.primary_key_val);
        const chunkSize = CONFIG.upsertChunkSize;
        for (let i = 0; i < idsToDelete.length; i += chunkSize) {
          await trx(tableName)
            .whereIn(pkCol, idsToDelete.slice(i, i + chunkSize))
            .del();
        }
      } else {
        await this.bulkDeleteComposite(trx, tableName, pkColumns, deletes);
      }
    }

    if (upserts.length > 0) {
      const chunkSize = CONFIG.upsertChunkSize;
      for (let i = 0; i < upserts.length; i += chunkSize) {
        await trx(tableName)
          .insert(upserts.slice(i, i + chunkSize))
          .onConflict(pkColumns)
          .merge();
      }
    }
  }

  async processBatch() {
    if (this.isProcessing) return false;
    this.isProcessing = true;
    try {
      const lastId = await this.getCheckpoint();
      const changes = await this.source
        .db("sym_change_log")
        .select(
          "change_id",
          "table_name",
          "operation",
          "primary_key_val",
          "row_data",
          "node_source_id",
        )
        .where("change_id", ">", lastId.toString())
        .whereNot("node_source_id", this.target.id)
        .orderBy("change_id", "asc")
        .limit(CONFIG.batchSize);

      if (changes.length === 0) return false;

      const byTable = new Map();
      let validEventCount = 0;
      let skippedEventCount = 0;

      for (const change of changes) {
        if (!ALLOWED_TABLES.has(change.table_name)) {
          skippedEventCount++;
          continue;
        }
        if (!byTable.has(change.table_name)) byTable.set(change.table_name, []);
        byTable.get(change.table_name).push(change);
        validEventCount++;
      }

      const lastBatchId = changes[changes.length - 1].change_id;

      if (validEventCount === 0) {
        await this.setCheckpoint(lastBatchId);
        return changes.length === CONFIG.batchSize;
      }

      let restoreSession = async () => {};
      let appliedCount = 0;
      let quarantineCount = 0;

      try {
        await this.target.db.transaction(async (trx) => {
          restoreSession = await this.setSessionBypass(trx);
          for (const [tableName, tableChanges] of byTable) {
            try {
              await this.applyTableChanges(trx, tableName, tableChanges);
              appliedCount += tableChanges.length;
            } catch (tableErr) {
              logger.error(
                `[Force Sync] Table-level apply failed for ${tableName} ` +
                  `(${this.source.id} -> ${this.target.id}): ${tableErr.message}. Falling back to per-row.`,
              );
              for (const change of tableChanges) {
                try {
                  await this.applyTableChanges(trx, tableName, [change]);
                  appliedCount++;
                } catch (rowErr) {
                  quarantineCount++;
                  this.stats.lastQuarantineChangeId = change.change_id;
                  logger.error(
                    `[Quarantine] change_id=${change.change_id} table=${tableName} ` +
                      `op=${change.operation} pk=${change.primary_key_val}: ${rowErr.message}`,
                  );
                }
              }
            }
          }
          await this.setCheckpoint(lastBatchId, trx);
        });
      } finally {
        await restoreSession();
      }

      this.stats.appliedEvents += appliedCount;
      this.stats.quarantinedEvents += quarantineCount;
      this.stats.skippedEvents += skippedEventCount;
      this.stats.batches++;
      this.stats.lastAppliedAt = new Date();

      logger.info(
        `[${this.source.id} -> ${this.target.id}] applied=${appliedCount} ` +
          `quarantined=${quarantineCount} skipped=${skippedEventCount} checkpoint=${lastBatchId}`,
      );

      return changes.length === CONFIG.batchSize;
    } finally {
      this.isProcessing = false;
    }
  }
}

/* ------------------------------------------------------------------ */
/* Node builders (mirrors server.js)                                  */
/* ------------------------------------------------------------------ */

function buildHubNode() {
  const client = normalizeClient(process.env.LOCAL_CLIENT);
  return {
    id: process.env.LOCAL_ID || "NODE_MAIN_HUB",
    client,
    db: knex({
      client,
      connection: buildKnexConnection(client, {
        url: process.env.LOCAL_URL,
        ssl: parseBoolEnv("LOCAL_SSL", false),
        sslStrict: parseBoolEnv("LOCAL_SSL_STRICT", true),
      }),
      pool: { min: 0, max: parseIntEnv("FORCE_SYNC_POOL_MAX", 10, { min: 1 }) },
    }),
  };
}

function buildBranchNodes(filterIds) {
  let cfg = [];
  try {
    cfg = JSON.parse(process.env.BRANCHES_JSON || "[]");
    if (!Array.isArray(cfg))
      throw new Error("BRANCHES_JSON must be a JSON array");
  } catch (err) {
    logger.error(`CRITICAL: Invalid BRANCHES_JSON: ${err.message}`);
    process.exit(1);
  }

  for (const b of cfg) {
    if (!b.id || !b.url) {
      logger.error(
        `CRITICAL: Each BRANCHES_JSON entry needs "id" and "url". Got: ${JSON.stringify(b)}`,
      );
      process.exit(1);
    }
  }

  const filtered =
    filterIds.length > 0 ? cfg.filter((b) => filterIds.includes(b.id)) : cfg;

  if (filterIds.length > 0 && filtered.length !== filterIds.length) {
    const found = new Set(filtered.map((b) => b.id));
    const missing = filterIds.filter((id) => !found.has(id));
    logger.error(`CRITICAL: Unknown branch id(s): ${missing.join(", ")}`);
    process.exit(1);
  }

  return filtered.map((b) => {
    const client = normalizeClient(b.client);
    return {
      id: b.id,
      client,
      db: knex({
        client,
        connection: buildKnexConnection(client, {
          url: b.url,
          ssl: true,
          sslStrict: b.sslInsecure !== true,
        }),
        pool: {
          min: 0,
          max: parseIntEnv("FORCE_SYNC_POOL_MAX", 10, { min: 1 }),
        },
      }),
    };
  });
}

/* ------------------------------------------------------------------ */
/* Provision (schema only — safe to run even if server already did)   */
/* ------------------------------------------------------------------ */

async function provisionNode(node) {
  const db = node.db;
  const client = node.client;

  if (isMysqlClient(client)) {
    try {
      await db.raw("SET GLOBAL log_bin_trust_function_creators = 1;");
    } catch (err) {
      logger.debug(
        `[Provision] Skip log_bin_trust_function_creators on ${node.id}: ${err.message}`,
      );
    }
  }

  if (!(await db.schema.hasTable("sym_change_log"))) {
    await db.schema.createTable("sym_change_log", (table) => {
      table.bigIncrements("change_id").primary();
      table.string("table_name", 100).notNullable();
      table.string("operation", 10).notNullable();
      table.string("primary_key_val", 255).notNullable();
      table.text("row_data").notNullable();
      table.string("node_source_id", 50).notNullable();
      table.timestamp("created_at").defaultTo(db.fn.now());
    });
    logger.info(`[Provision] Created sym_change_log on [${node.id}]`);
  }

  if (!(await db.schema.hasTable("sym_checkpoint"))) {
    await db.schema.createTable("sym_checkpoint", (table) => {
      table.string("node_id", 50).primary();
      table.bigInteger("last_processed_change_id").notNullable();
      table.timestamp("updated_at").defaultTo(db.fn.now());
    });
    logger.info(`[Provision] Created sym_checkpoint on [${node.id}]`);
  }

  if (isPgClient(client)) {
    await db.raw(
      "CREATE INDEX IF NOT EXISTS idx_sym_log_sync_poll ON sym_change_log (change_id ASC, node_source_id);",
    );
    await db.raw(
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_sym_checkpoint_node ON sym_checkpoint (node_id);",
    );
  } else if (isMysqlClient(client)) {
    try {
      await db.raw(
        "CREATE INDEX idx_sym_log_sync_poll ON sym_change_log (change_id ASC, node_source_id);",
      );
    } catch (err) {
      if (!/Duplicate key name/i.test(err.message)) throw err;
    }
  }

  // Do NOT auto-setup triggers here unless explicitly asked; force-sync is
  // about catching up existing backlog, not changing write-side behavior.
  if (parseBoolEnv("SETUP_TRIGGERS_ON_STARTUP", false)) {
    await setupNodeTriggers(node);
  }
}

/* ------------------------------------------------------------------ */
/* Force-sync one branch, both directions, until caught up            */
/* ------------------------------------------------------------------ */

async function drainWorker(worker, label) {
  let batchCount = 0;
  while (true) {
    if (batchCount >= CONFIG.forceMaxBatches) {
      throw new Error(
        `[${label}] exceeded FORCE_SYNC_MAX_BATCHES (${CONFIG.forceMaxBatches}) — not converging`,
      );
    }
    const hadMore = await worker.processBatch();
    if (hadMore) {
      batchCount++;
      await new Promise((r) => setImmediate(r));
      continue;
    }
    return batchCount;
  }
}

async function forceSyncBranch(hub, branch) {
  logger.info(`=== Force syncing branch [${branch.id}] ===`);

  const hubToBranch = new SyncWorker(hub, branch);
  const branchToHub = new SyncWorker(branch, hub);

  // Snapshot pre-state for reporting.
  const branchCheckpointBefore = await hubToBranch.getCheckpoint();
  const hubCheckpointBefore = await branchToHub.getCheckpoint();

  // Pass 1: hub -> branch
  const hubToBranchBatches = await drainWorker(
    hubToBranch,
    `${hub.id}->${branch.id}`,
  );
  // Pass 2: branch -> hub
  const branchToHubBatches = await drainWorker(
    branchToHub,
    `${branch.id}->${hub.id}`,
  );

  // Verify catch-up by re-checking source max vs checkpoint.
  const hubMax = await hubToBranch.getSourceMaxChangeId();
  const branchCheckpointAfter = await hubToBranch.getCheckpoint();

  const branchMax = await branchToHub.getSourceMaxChangeId();
  const hubCheckpointAfter = await branchToHub.getCheckpoint();

  const hubConverged = branchCheckpointAfter >= hubMax;
  const branchConverged = hubCheckpointAfter >= branchMax;

  const report = {
    branchId: branch.id,
    hubToBranch: {
      batches: hubToBranchBatches,
      checkpointBefore: branchCheckpointBefore.toString(),
      checkpointAfter: branchCheckpointAfter.toString(),
      sourceMax: hubMax.toString(),
      converged: hubConverged,
      applied: hubToBranch.stats.appliedEvents,
      skipped: hubToBranch.stats.skippedEvents,
      quarantined: hubToBranch.stats.quarantinedEvents,
      lastQuarantineChangeId: hubToBranch.stats.lastQuarantineChangeId,
    },
    branchToHub: {
      batches: branchToHubBatches,
      checkpointBefore: hubCheckpointBefore.toString(),
      checkpointAfter: hubCheckpointAfter.toString(),
      sourceMax: branchMax.toString(),
      converged: branchConverged,
      applied: branchToHub.stats.appliedEvents,
      skipped: branchToHub.stats.skippedEvents,
      quarantined: branchToHub.stats.quarantinedEvents,
      lastQuarantineChangeId: branchToHub.stats.lastQuarantineChangeId,
    },
    converged: hubConverged && branchConverged,
  };

  return report;
}

/* ------------------------------------------------------------------ */
/* Main                                                               */
/* ------------------------------------------------------------------ */

async function main() {
  // Validate encryption key format if present (mirrors server.js).
  if (CONFIG.encryptionKey) {
    let keyBuf;
    try {
      keyBuf = Buffer.from(CONFIG.encryptionKey, "hex");
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

  const filterIds = process.argv.slice(2).filter((a) => !a.startsWith("-"));

  const hub = buildHubNode();
  const branches = buildBranchNodes(filterIds);

  if (branches.length === 0) {
    logger.error("No branches configured. Set BRANCHES_JSON.");
    process.exit(1);
  }

  logger.info(
    `Force-sync starting: hub=[${hub.id}] branches=[${branches
      .map((b) => b.id)
      .join(", ")}]`,
  );

  // Ensure schemas exist before we touch checkpoints.
  await provisionNode(hub);
  for (const b of branches) {
    try {
      await provisionNode(b);
    } catch (err) {
      logger.error(
        `CRITICAL: Branch [${b.id}] unreachable during provisioning: ${err.message}`,
      );
      process.exit(1);
    }
  }

  const reports = [];
  let allConverged = true;

  for (const branch of branches) {
    try {
      const report = await forceSyncBranch(hub, branch);
      reports.push(report);
      if (!report.converged) allConverged = false;
    } catch (err) {
      allConverged = false;
      reports.push({
        branchId: branch.id,
        error: err.message,
        converged: false,
      });
      logger.error(
        `[${branch.id}] force-sync failed: ${err.stack || err.message}`,
      );
    }
  }

  // Cleanup connections.
  try {
    await hub.db.destroy();
  } catch (err) {
    logger.error(`Error closing hub DB: ${err.message}`);
  }
  await Promise.allSettled(branches.map((b) => b.db.destroy()));

  logger.info("=== Force-sync summary ===");
  for (const r of reports) {
    logger.info(JSON.stringify(r, null, 2));
  }

  if (!allConverged) {
    logger.error("Force-sync did NOT fully converge. See summary above.");
    process.exit(1);
  }

  logger.info("Force-sync complete: all branches converged.");
  process.exit(0);
}

main().catch((err) => {
  logger.error(`[Fatal]: ${err.stack || err.message}`);
  process.exit(1);
});
