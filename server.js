require("dotenv").config();
const express = require("express");
const knex = require("knex");
const winston = require("winston");
const crypto = require("crypto");
const cors = require("cors");
const rateLimit = require("express-rate-limit");

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

function parseBoolEnv(name, defaultValue = false) {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return defaultValue;
  return raw === "true" || raw === "1";
}

/* ------------------------------------------------------------------ */
/* Secrets / auth                                                     */
/* ------------------------------------------------------------------ */

const API_SECRET_KEY = requireEnv("API_SECRET_KEY");
const API_SECRET_KEY_HASH = crypto
  .createHash("sha256")
  .update(API_SECRET_KEY)
  .digest();

let ALLOWED_COLUMNS = {};
try {
  ALLOWED_COLUMNS = JSON.parse(process.env.ALLOWED_COLUMNS_JSON || "{}");
} catch (err) {
  logger.error(`Invalid ALLOWED_COLUMNS_JSON, ignoring: ${err.message}`);
}

// Warn if a synced table has no column allowlist — this is a security
// boundary, not just a filter.
for (const table of ALLOWED_TABLES) {
  if (!Array.isArray(ALLOWED_COLUMNS[table])) {
    logger.warn(
      `[Config] ALLOWED_COLUMNS has no entry for synced table "${table}". ` +
        `All payload columns will be accepted — consider adding an allowlist.`,
    );
  }
}

const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((o) => o.trim())
  .filter(Boolean);

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

function buildKnexConnection(nodeClient, opts) {
  // opts: { url, ssl, sslStrict, isBranch }
  if (isPgClient(nodeClient)) {
    return {
      connectionString: opts.url,
      ssl: opts.ssl ? { rejectUnauthorized: opts.sslStrict !== false } : false,
    };
  }
  return parseConnectionString(opts.url, defaultPortFor(nodeClient));
}

/* ------------------------------------------------------------------ */
/* Hub node                                                           */
/* ------------------------------------------------------------------ */

const hubClient = normalizeClient(process.env.LOCAL_CLIENT);
const HUB_NODE = {
  id: process.env.LOCAL_ID || "NODE_MAIN_HUB",
  client: hubClient,
  db: knex({
    client: hubClient,
    connection: buildKnexConnection(hubClient, {
      url: process.env.LOCAL_URL,
      ssl: parseBoolEnv("LOCAL_SSL", false),
      sslStrict: parseBoolEnv("LOCAL_SSL_STRICT", true),
    }),
    pool: {
      min: parseIntEnv("HUB_POOL_MIN", 4, { min: 0, max: 200 }),
      max: parseIntEnv("HUB_POOL_MAX", 50, { min: 1, max: 200 }),
      acquireTimeoutMillis: parseIntEnv("HUB_POOL_ACQUIRE_TIMEOUT_MS", 10000, {
        min: 1000,
      }),
      idleTimeoutMillis: parseIntEnv("HUB_POOL_IDLE_TIMEOUT_MS", 30000, {
        min: 5000,
      }),
    },
  }),
};

/* ------------------------------------------------------------------ */
/* Branch nodes                                                       */
/* ------------------------------------------------------------------ */

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
        ssl: true, // branches always pass SSL config; the knex connection respects it
        sslStrict: b.sslInsecure !== true,
      }),
      pool: {
        min: parseIntEnv("BRANCH_POOL_MIN", 0, { min: 0, max: 200 }),
        max: parseIntEnv("BRANCH_POOL_MAX", 10, { min: 1, max: 200 }),
        acquireTimeoutMillis: parseIntEnv(
          "BRANCH_POOL_ACQUIRE_TIMEOUT_MS",
          10000,
          { min: 1000 },
        ),
        idleTimeoutMillis: parseIntEnv("BRANCH_POOL_IDLE_TIMEOUT_MS", 30000, {
          min: 5000,
        }),
      },
    }),
  };
});

/* ------------------------------------------------------------------ */
/* Config                                                             */
/* ------------------------------------------------------------------ */

const CONFIG = {
  port: parseIntEnv("PORT", 3000, { min: 1, max: 65535 }),
  pollIntervalMs: parseIntEnv("POLL_INTERVAL_MS", 2000, { min: 250 }),
  batchSize: parseIntEnv("BATCH_SIZE", 500, { min: 1, max: 5000 }),
  upsertChunkSize: parseIntEnv("UPSERT_CHUNK_SIZE", 200, { min: 1, max: 2000 }),
  encryptionKey: process.env.PAYLOAD_ENCRYPTION_KEY || null,
  setupTriggersOnStartup: parseBoolEnv("SETUP_TRIGGERS_ON_STARTUP", false),
  maxBackoffMs: parseIntEnv("MAX_BACKOFF_MS", 30000, { min: 1000 }),
  checkpointValidationEvery: parseIntEnv("CHECKPOINT_VALIDATION_EVERY", 1, {
    min: 1,
  }),
  compositeDeleteChunkSize: parseIntEnv("COMPOSITE_DELETE_CHUNK_SIZE", 200, {
    min: 1,
    max: 1000,
  }),
  flushMaxIterations: parseIntEnv("FLUSH_MAX_ITERATIONS", 100000, { min: 1 }),
  shutdownTimeoutMs: parseIntEnv("SHUTDOWN_TIMEOUT_MS", 10000, { min: 2000 }),
};

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
      // Not valid JSON — if encryption is on, this is unexpected.
      throw new Error("Payload is not a valid encrypted envelope");
    }

    if (
      !parsed ||
      typeof parsed !== "object" ||
      !parsed.iv ||
      !parsed.encrypted ||
      !parsed.authTag
    ) {
      // Looks like plaintext, but encryption is configured — refuse.
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
/* Provisioning                                                       */
/* ------------------------------------------------------------------ */

async function provisionNode(node) {
  const db = node.db;
  const client = node.client;

  if (isMysqlClient(client)) {
    try {
      await db.raw("SET GLOBAL log_bin_trust_function_creators = 1;");
    } catch (err) {
      logger.debug(
        `[Provision] Skip setting log_bin_trust_function_creators on ${node.id}: ${err.message}`,
      );
    }
  }

  const hasChangeLog = await db.schema.hasTable("sym_change_log");
  if (!hasChangeLog) {
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

  const hasCheckpoint = await db.schema.hasTable("sym_checkpoint");
  if (!hasCheckpoint) {
    await db.schema.createTable("sym_checkpoint", (table) => {
      table.string("node_id", 50).primary();
      table.bigInteger("last_processed_change_id").notNullable();
      table.timestamp("updated_at").defaultTo(db.fn.now());
    });
    logger.info(`[Provision] Created sym_checkpoint on [${node.id}]`);
  }

  // Indexes — idempotent for both clients.
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
    // MySQL: node_id is primary already, no extra unique index needed.
  }

  if (CONFIG.setupTriggersOnStartup) {
    await setupNodeTriggers(node);
  } else {
    logger.info(
      `[Provision] Skipping trigger setup on node [${node.id}] (SETUP_TRIGGERS_ON_STARTUP is not true).`,
    );
  }
}

/* ------------------------------------------------------------------ */
/* PK cache (promise-cached, failure-safe)                            */
/* ------------------------------------------------------------------ */

const PK_CACHE = new Map(); // key -> Promise<string[]>

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
    // If it rejects, remove from cache so the next call retries.
    promise.catch(() => PK_CACHE.delete(cacheKey));
    PK_CACHE.set(cacheKey, promise);
  }
  return PK_CACHE.get(cacheKey);
}

/* ------------------------------------------------------------------ */
/* Sync worker                                                        */
/* ------------------------------------------------------------------ */

class SyncWorker {
  constructor(source, target) {
    this.source = source;
    this.target = target;
    this.isProcessing = false;
    this.checkpointCallCount = 0;
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

    let currentCheckpoint = record
      ? BigInt(record.last_processed_change_id)
      : 0n;

    this.checkpointCallCount++;
    const shouldValidate =
      this.checkpointCallCount % CONFIG.checkpointValidationEvery === 0;

    if (!shouldValidate) return currentCheckpoint;

    const maxRow = await this.source
      .db("sym_change_log")
      .max("change_id as max_id")
      .first();

    const maxSourceId = maxRow?.max_id != null ? BigInt(maxRow.max_id) : 0n;

    if (currentCheckpoint > maxSourceId) {
      logger.warn(
        `[Checkpoint Resync] ${this.source.id} -> ${this.target.id}: ` +
          `Checkpoint (${currentCheckpoint}) is ahead of source max ID (${maxSourceId}). Resetting...`,
      );
      currentCheckpoint = maxSourceId;
      await this.setCheckpoint(currentCheckpoint);
    }

    return currentCheckpoint;
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

  /**
   * Apply session-level bypasses. Returns a restore function that MUST be
   * called in a finally block to avoid leaking state on pooled connections.
   */
  async setSessionBypass(trx) {
    const client = this.target.client;
    if (isPgClient(client)) {
      await trx.raw("SET LOCAL sym.is_syncing = 'true'");
      return async () => {}; // SET LOCAL is scoped to the transaction
    }
    if (isMysqlClient(client)) {
      await trx.raw("SET @sym_is_syncing = TRUE;");
      await trx.raw("SET FOREIGN_KEY_CHECKS = 0;");
      await trx.raw(
        "SET SESSION sql_mode = REPLACE(REPLACE(@@sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '');",
      );
      return async () => {
        // Restore on the SAME connection (the trx connection). If the trx
        // already rolled back, these may fail — swallow and log, because the
        // connection is being returned to the pool and we must not leave
        // FOREIGN_KEY_CHECKS=0 behind.
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
        // Anchored match: only null out the sentinel itself, not strings
        // that merely contain it.
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
        const payload = this.decodePayload(change);
        upserts.push(payload);
      }
    }

    if (deletes.length > 0) {
      if (pkColumns.length === 1) {
        const pkCol = pkColumns[0];
        const idsToDelete = deletes.map((d) => d.primary_key_val);
        // Chunk whereIn to avoid massive IN lists.
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
      // Chunk inserts to avoid exceeding driver parameter limits on huge batches.
      const chunkSize = CONFIG.upsertChunkSize;
      for (let i = 0; i < upserts.length; i += chunkSize) {
        await trx(tableName)
          .insert(upserts.slice(i, i + chunkSize))
          .onConflict(pkColumns)
          .merge();
      }
    }
  }

  /**
   * Process one batch. Returns true if a full batch was processed (more may
   * be waiting), false otherwise.
   *
   * Poison rows: a single row failing to decode/apply no longer stalls the
   * branch. We log, count, and skip past it (advancing the checkpoint), so
   * the branch catches up. The quarantined change_id is recorded for
   * operator inspection.
   */
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

      if (skippedEventCount > 0) {
        this.stats.skippedEvents += skippedEventCount;
        logger.warn(
          `[Sync Engine] ${this.source.id} -> ${this.target.id} | Skipped ${skippedEventCount} events for disallowed tables`,
        );
      }

      const lastBatchId = changes[changes.length - 1].change_id;

      if (validEventCount === 0) {
        await this.setCheckpoint(lastBatchId);
        return changes.length === CONFIG.batchSize;
      }

      logger.debug(
        `[Sync Engine] ${this.source.id} -> ${this.target.id} | Processing ${validEventCount} events across ${byTable.size} tables (checkpoint -> ${lastBatchId})...`,
      );

      let restoreSession = async () => {};
      let appliedCount = 0;
      let quarantineCount = 0;

      try {
        await this.target.db.transaction(async (trx) => {
          restoreSession = await this.setSessionBypass(trx);

          for (const [tableName, tableChanges] of byTable) {
            // Per-table try/catch: a single bad table doesn't roll back the
            // whole batch. We fall back to per-row apply for that table so
            // the good rows still land.
            try {
              await this.applyTableChanges(trx, tableName, tableChanges);
              appliedCount += tableChanges.length;
            } catch (tableErr) {
              logger.error(
                `[Sync Engine] Table-level apply failed for ${tableName} ` +
                  `(${this.source.id} -> ${this.target.id}, change_ids ${tableChanges[0].change_id}..${tableChanges[tableChanges.length - 1].change_id}): ${tableErr.message}. ` +
                  `Falling back to per-row apply.`,
              );
              for (const change of tableChanges) {
                try {
                  await this.applyTableChanges(trx, tableName, [change]);
                  appliedCount++;
                } catch (rowErr) {
                  quarantineCount++;
                  this.stats.lastQuarantineChangeId = change.change_id;
                  logger.error(
                    `[Quarantine] change_id=${change.change_id} table=${tableName} op=${change.operation} ` +
                      `pk=${change.primary_key_val}: ${rowErr.message}`,
                  );
                }
              }
            }
          }

          await this.setCheckpoint(lastBatchId, trx);
        });
      } finally {
        // Must run on the same connection the trx used. If the trx already
        // closed it, this is a no-op with a log — never leaves FK checks off.
        await restoreSession();
      }

      this.stats.appliedEvents += appliedCount;
      this.stats.quarantinedEvents += quarantineCount;
      this.stats.batches++;
      this.stats.lastAppliedAt = new Date();

      logger.info(
        `[Sync Engine] ${this.source.id} -> ${this.target.id} | Applied ${appliedCount}, ` +
          `quarantined ${quarantineCount}, skipped ${skippedEventCount} (checkpoint ${lastBatchId})`,
      );

      return changes.length === CONFIG.batchSize;
    } finally {
      this.isProcessing = false;
    }
  }
}

/* ------------------------------------------------------------------ */
/* Branch sync manager                                                */
/* ------------------------------------------------------------------ */

class BranchSyncManager {
  constructor(branchId, hubToBranch, branchToHub, opts = {}) {
    this.branchId = branchId;
    this.hubToBranch = hubToBranch;
    this.branchToHub = branchToHub;
    this.pollIntervalMs = opts.pollIntervalMs || CONFIG.pollIntervalMs;
    this.maxBackoffMs = opts.maxBackoffMs || CONFIG.maxBackoffMs;
    this.running = true;
    this.consecutiveErrors = 0;
    this.lastRunAt = null;
    this.lastError = null;
    this.lastErrorAt = null;
  }

  async loop() {
    while (this.running) {
      let hadWork = false;
      let hadError = false;

      try {
        const [hubMore, branchMore] = await Promise.all([
          this.hubToBranch.processBatch(),
          this.branchToHub.processBatch(),
        ]);
        hadWork = hubMore || branchMore;
        if (this.consecutiveErrors > 0) {
          logger.info(
            `[${this.branchId}] recovered after ${this.consecutiveErrors} consecutive error(s)`,
          );
        }
        this.consecutiveErrors = 0;
        this.lastError = null;
      } catch (err) {
        hadError = true;
        this.consecutiveErrors++;
        this.lastError = err.message;
        this.lastErrorAt = new Date();
        logger.error(
          `[${this.branchId}] sync error (#${this.consecutiveErrors}): ${err.stack || err.message}`,
        );
      }

      this.lastRunAt = new Date();
      if (!this.running) break;

      if (hadWork && !hadError) {
        // Yield to event loop so the process stays responsive under load.
        await new Promise((resolve) => setImmediate(resolve));
        continue;
      }

      const baseDelay = hadError
        ? Math.min(
            this.pollIntervalMs * 2 ** Math.min(this.consecutiveErrors, 6),
            this.maxBackoffMs,
          )
        : this.pollIntervalMs;

      const delay = hadError
        ? baseDelay * (0.85 + Math.random() * 0.3)
        : baseDelay;
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  stop() {
    this.running = false;
  }

  status() {
    return {
      branchId: this.branchId,
      running: this.running,
      consecutiveErrors: this.consecutiveErrors,
      lastRunAt: this.lastRunAt,
      lastError: this.lastError,
      lastErrorAt: this.lastErrorAt,
      hubToBranch: { ...this.hubToBranch.stats },
      branchToHub: { ...this.branchToHub.stats },
    };
  }
}

/* ------------------------------------------------------------------ */
/* Startup flush (bounded)                                            */
/* ------------------------------------------------------------------ */

async function flushPendingSyncs(workers) {
  logger.info(
    "[Startup Sync] Checking and draining pending backlog across all nodes...",
  );
  let batchCount = 0;
  let hasMoreData = true;

  while (hasMoreData) {
    if (batchCount >= CONFIG.flushMaxIterations) {
      logger.warn(
        `[Startup Sync] Reached FLUSH_MAX_ITERATIONS (${CONFIG.flushMaxIterations}); ` +
          `continuing in background.`,
      );
      break;
    }

    const tasks = workers.flatMap((w) => [
      w.hubToBranch.processBatch().catch((err) => {
        logger.error(
          `[Flush Error] ${w.branchId} hub->branch: ${err.stack || err.message}`,
        );
        return false;
      }),
      w.branchToHub.processBatch().catch((err) => {
        logger.error(
          `[Flush Error] ${w.branchId} branch->hub: ${err.stack || err.message}`,
        );
        return false;
      }),
    ]);

    const results = await Promise.all(tasks);
    hasMoreData = results.some((hasMore) => hasMore === true);

    if (hasMoreData) {
      batchCount++;
      if (batchCount % 50 === 0) {
        logger.info(`[Startup Sync] Drained ${batchCount} batches so far...`);
      }
      await new Promise((resolve) => setImmediate(resolve));
    }
  }

  logger.info(`[Startup Sync] Flush complete. Drained ${batchCount} batches.`);
}

/* ------------------------------------------------------------------ */
/* HTTP layer                                                         */
/* ------------------------------------------------------------------ */

const app = express();

if (parseBoolEnv("TRUST_PROXY", false)) app.set("trust proxy", 1);

if (ALLOWED_ORIGINS.length > 0) {
  app.use(cors({ origin: ALLOWED_ORIGINS }));
}

app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  next();
});

app.use(
  rateLimit({
    windowMs: 60 * 1000,
    max: parseIntEnv("RATE_LIMIT_MAX", 60, { min: 1 }),
    standardHeaders: true,
    legacyHeaders: false,
  }),
);

app.use(express.json({ limit: "100kb" }));

app.use((req, res, next) => {
  const start = Date.now();
  res.on("finish", () => {
    logger.debug(
      `[HTTP] ${req.method} ${req.path} ${res.statusCode} ${Date.now() - start}ms (${req.ip})`,
    );
  });
  next();
});

function requireApiKey(req, res, next) {
  const provided = req.get("x-api-key") || "";
  const providedHash = crypto.createHash("sha256").update(provided).digest();
  const valid =
    providedHash.length === API_SECRET_KEY_HASH.length &&
    crypto.timingSafeEqual(providedHash, API_SECRET_KEY_HASH);
  if (!valid) {
    logger.warn(
      `[Auth] Rejected request to ${req.path} from ${req.ip} - invalid API key`,
    );
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

let syncManagers = [];
let managerById = new Map();

app.get("/health", requireApiKey, async (req, res) => {
  let hubMaxChangeId = null;
  try {
    const row = await HUB_NODE.db("sym_change_log")
      .max("change_id as m")
      .first();
    hubMaxChangeId = row?.m != null ? BigInt(row.m) : 0n;
  } catch (err) {
    logger.error(`[Health] Failed to read hub max change_id: ${err.message}`);
  }

  const branchStatus = await Promise.all(
    branchNodes.map(async (branch) => {
      const manager = managerById.get(branch.id);
      try {
        const lastCheck = await HUB_NODE.db("sym_checkpoint")
          .where("node_id", branch.id)
          .first();

        const lastProcessed = lastCheck?.last_processed_change_id
          ? BigInt(lastCheck.last_processed_change_id)
          : 0n;

        const lag =
          hubMaxChangeId != null
            ? (hubMaxChangeId - lastProcessed).toString()
            : null;

        return {
          branchId: branch.id,
          status: "CONNECTED",
          lastProcessedChangeId: lastProcessed.toString(),
          hubToBranchLag: lag,
          healthy: manager ? manager.consecutiveErrors === 0 : false,
          consecutiveErrors: manager?.consecutiveErrors ?? null,
          lastError: manager?.lastError ?? null,
          lastErrorAt: manager?.lastErrorAt ?? null,
          lastRunAt: manager?.lastRunAt ?? null,
          stats: manager?.status() ?? null,
        };
      } catch (err) {
        return {
          branchId: branch.id,
          status: "OFFLINE/UNREACHABLE",
          error: err.message,
          healthy: false,
          consecutiveErrors: manager?.consecutiveErrors ?? null,
        };
      }
    }),
  );

  const allHealthy = branchStatus.every((b) => b.healthy !== false);

  res.json({
    hub: HUB_NODE.id,
    healthy: allHealthy,
    activeBranches: branchStatus.length,
    branches: branchStatus,
  });
});

/* ------------------------------------------------------------------ */
/* Lifecycle                                                          */
/* ------------------------------------------------------------------ */

let isShuttingDown = false;
let server;

async function startMultiBranchEngine() {
  try {
    logger.info(`Starting Hub-and-Spoke Engine for Hub [${HUB_NODE.id}]...`);
    logger.info(
      `[Config] port=${CONFIG.port} pollIntervalMs=${CONFIG.pollIntervalMs} ` +
        `batchSize=${CONFIG.batchSize} branches=${branchNodes.length} ` +
        `encryptionEnabled=${Boolean(CONFIG.encryptionKey)}`,
    );

    await provisionNode(HUB_NODE);

    const flushList = [];

    for (const branch of branchNodes) {
      try {
        await provisionNode(branch);
        logger.info(
          `[Startup] Provisioned schema and settings for branch: [${branch.id}]`,
        );
      } catch (err) {
        logger.warn(
          `[Startup] Branch [${branch.id}] unreachable during startup provisioning: ${err.message}`,
        );
      }

      const hubToBranch = new SyncWorker(HUB_NODE, branch);
      const branchToHub = new SyncWorker(branch, HUB_NODE);
      flushList.push({ branchId: branch.id, hubToBranch, branchToHub });

      const manager = new BranchSyncManager(
        branch.id,
        hubToBranch,
        branchToHub,
      );
      syncManagers.push(manager);
      managerById.set(branch.id, manager);

      logger.info(`Registered sync worker for branch: [${branch.id}]`);
    }

    await flushPendingSyncs(flushList);

    server = app.listen(CONFIG.port, () => {
      logger.info(`Hub Monitoring API active on port ${CONFIG.port}`);
    });

    syncManagers.forEach((m) => {
      m.loop().catch((err) => {
        logger.error(
          `[Fatal Branch Loop Error] ${m.branchId}: ${err.stack || err.message}`,
        );
      });
    });

    logger.info(
      `All ${syncManagers.length} branch sync loops launched independently.`,
    );
  } catch (err) {
    logger.error(`[Fatal Hub Engine Error]: ${err.stack}`);
    process.exit(1);
  }
}

async function shutdown(signal) {
  if (isShuttingDown) {
    logger.info("Forced shutdown requested. Exiting now.");
    process.exit(1);
  }
  isShuttingDown = true;
  logger.info(`Received ${signal}, shutting down gracefully...`);

  syncManagers.forEach((m) => m.stop());

  const timer = setTimeout(() => {
    logger.error("Forcefully shutting down due to timeout.");
    process.exit(1);
  }, CONFIG.shutdownTimeoutMs);
  timer.unref();

  if (server) {
    server.close();
    if (typeof server.closeIdleConnections === "function")
      server.closeIdleConnections();
  }

  try {
    await HUB_NODE.db.destroy();
  } catch (err) {
    logger.error(`Error closing hub DB connections: ${err.message}`);
  }

  const results = await Promise.allSettled(
    branchNodes.map((b) => b.db.destroy()),
  );
  results.forEach((r, i) => {
    if (r.status === "rejected") {
      logger.error(
        `Error closing branch [${branchNodes[i].id}] DB connections: ${r.reason?.message}`,
      );
    }
  });

  clearTimeout(timer);
  logger.info("Shutdown complete.");
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

process.on("unhandledRejection", (reason) => {
  logger.error(`[Unhandled Rejection]: ${reason?.stack || reason}`);
});

process.on("uncaughtException", (err) => {
  logger.error(`[Uncaught Exception]: ${err.stack || err.message}`);
  shutdown("uncaughtException");
});

startMultiBranchEngine();
