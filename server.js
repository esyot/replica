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
        ssl: true,
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

  // Defer-retry for transient FK failures (cross-batch dependencies)
  deferMaxAttempts: parseIntEnv("DEFER_MAX_ATTEMPTS", 5, { min: 1, max: 100 }),
  deferMaxQueueSize: parseIntEnv("DEFER_MAX_QUEUE_SIZE", 10000, { min: 1 }),

  // Dependency-order cache TTL
  dependencyOrderTtlMs: parseIntEnv("DEPENDENCY_ORDER_TTL_MS", 60000, {
    min: 1000,
  }),

  // Health endpoint caching for row-count estimates
  healthCacheTtlMs: parseIntEnv("HEALTH_CACHE_TTL_MS", 30000, { min: 1000 }),
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

  if (CONFIG.setupTriggersOnStartup) {
    await setupNodeTriggers(node);
  } else {
    logger.info(
      `[Provision] Skipping trigger setup on node [${node.id}] (SETUP_TRIGGERS_ON_STARTUP is not true).`,
    );
  }
}

/* ------------------------------------------------------------------ */
/* PK cache                                                           */
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
/* Dynamic FK-dependency order                                        */
/* ------------------------------------------------------------------ */

const DEP_ORDER_CACHE = new Map();

async function getDependencyOrder(node, tables) {
  const cacheKey = node.id;
  const cached = DEP_ORDER_CACHE.get(cacheKey);
  if (cached && Date.now() - cached.ts < CONFIG.dependencyOrderTtlMs) {
    const want = new Set(tables);
    return {
      order: cached.order.filter((t) => want.has(t)),
      edges: cached.edges,
    };
  }

  const db = node.db;
  const client = node.client;
  let rawEdges = [];

  try {
    if (isMysqlClient(client)) {
      const res = await db.raw(
        `SELECT TABLE_NAME AS child, REFERENCED_TABLE_NAME AS parent
         FROM information_schema.KEY_COLUMN_USAGE
         WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL`,
      );
      rawEdges = (res[0] || res).map((r) => ({
        child: r.child,
        parent: r.parent,
      }));
    } else if (isPgClient(client)) {
      const res = await db.raw(
        `SELECT kcu.table_name AS child, ccu.table_name AS parent
         FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
           ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
         JOIN information_schema.constraint_column_usage ccu
           ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
         WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = current_schema()`,
      );
      rawEdges = (res.rows || []).map((r) => ({
        child: r.child,
        parent: r.parent,
      }));
    }
  } catch (err) {
    logger.warn(
      `[table-order] FK graph query failed on [${node.id}] (${err.message}) — falling back to alphabetical`,
    );
    return { order: [...tables].sort(), edges: new Map() };
  }

  const inBatch = new Set(tables);
  const parentsOf = new Map();
  for (const t of tables) parentsOf.set(t, new Set());
  for (const e of rawEdges) {
    if (inBatch.has(e.child) && inBatch.has(e.parent)) {
      parentsOf.get(e.child).add(e.parent);
    }
  }

  const visited = new Set();
  const visiting = new Set();
  const order = [];
  const cyclePaths = [];

  function visit(table, stack) {
    if (visited.has(table)) return;
    if (visiting.has(table)) {
      const idx = stack.indexOf(table);
      if (idx >= 0) cyclePaths.push(stack.slice(idx).concat([table]));
      return;
    }
    visiting.add(table);
    stack.push(table);
    for (const parent of parentsOf.get(table) || []) visit(parent, stack);
    stack.pop();
    visiting.delete(table);
    visited.add(table);
    order.push(table);
  }

  for (const t of tables) visit(t, []);

  const realCycles = cyclePaths.filter((c) => new Set(c).size > 1);
  if (realCycles.length > 0) {
    const message = realCycles.map((c) => c.join(" -> ")).join("; ");
    throw new Error(
      `[table-order] Cross-table FK cycle on [${node.id}]: ${message}. ` +
        `Cannot order — sync of these tables will fail. Remove the cycle or contact a DBA.`,
    );
  }

  for (const t of tables) if (!visited.has(t)) order.push(t);

  const position = new Map(order.map((t, i) => [t, i]));
  for (const table of order) {
    for (const parent of parentsOf.get(table) || []) {
      if (!position.has(parent)) continue;
      if (position.get(parent) > position.get(table)) {
        throw new Error(
          `[table-order] Algorithm bug: ${table} (pos ${position.get(table)}) before parent ${parent} on [${node.id}]`,
        );
      }
    }
  }

  DEP_ORDER_CACHE.set(cacheKey, { order, edges: parentsOf, ts: Date.now() });
  return { order, edges: parentsOf };
}

function clearDependencyOrderCache(nodeId) {
  if (!nodeId) DEP_ORDER_CACHE.clear();
  else DEP_ORDER_CACHE.delete(nodeId);
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

    this.deferred = []; // [{ table, change, attempts }]

    this.halted = false;
    this.haltReason = null;
    this.haltedAt = null;

    this.stats = {
      appliedEvents: 0,
      skippedEvents: 0,
      quarantinedEvents: 0,
      deferredEvents: 0,
      resolvedDeferred: 0,
      batches: 0,
      lastAppliedAt: null,
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
          `checkpoint ${currentCheckpoint} ahead of source max ${maxSourceId}. Resetting.`,
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
      let restored = false;
      return async () => {
        if (restored) return;
        restored = true;
        try {
          await trx.raw("SET FOREIGN_KEY_CHECKS = 1;");
          await trx.raw("SET @sym_is_syncing = NULL;");
        } catch (err) {
          logger.debug(
            `[SessionBypass] Restore skipped on ${this.target.id}: ${err.message}`,
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
            pkColumns.forEach((col, idx) => this.andWhere(col, pkVals[idx]));
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

  isDeferrableFkError(err) {
    const msg = String(err?.message || err || "");
    return (
      /ER_NO_REFERENCED_ROW/i.test(msg) ||
      /Cannot add or update a child row: a foreign key constraint fails/i.test(
        msg,
      ) ||
      /foreign key constraint fails/i.test(msg)
    );
  }

  async retryDeferred(trx) {
    if (this.deferred.length === 0) return { resolved: 0, stillDeferred: [] };

    const tablesInDeferred = [...new Set(this.deferred.map((d) => d.table))];
    let tableOrder;
    try {
      const { order } = await getDependencyOrder(this.target, tablesInDeferred);
      tableOrder = order;
    } catch (err) {
      logger.error(`[Deferred] Order query failed: ${err.message}`);
      tableOrder = tablesInDeferred;
    }
    const orderIdx = new Map(tableOrder.map((t, i) => [t, i]));
    const sorted = [...this.deferred].sort(
      (a, b) =>
        (orderIdx.get(a.table) ?? 9999) - (orderIdx.get(b.table) ?? 9999),
    );

    const stillDeferred = [];
    let resolved = 0;

    for (const d of sorted) {
      try {
        await this.applyTableChanges(trx, d.table, [d.change]);
        resolved++;
        logger.info(
          `[Deferred] Resolved change_id=${d.change.change_id} table=${d.table} after ${d.attempts} attempt(s)`,
        );
      } catch (err) {
        d.attempts++;
        if (d.attempts >= CONFIG.deferMaxAttempts) {
          // HALT — don't quarantine
          this.halted = true;
          this.haltReason = `change_id=${d.change.change_id} table=${d.table} failed ${d.attempts} attempts: ${err.message}`;
          this.haltedAt = new Date();
          logger.error(
            `[HALT] ${this.source.id}->${this.target.id}: ${this.haltReason}`,
          );
          throw new Error(`Sync halted: ${this.haltReason}`);
        }
        stillDeferred.push(d);
      }
    }

    return { resolved, stillDeferred };
  }

  resume() {
    if (!this.halted) return false;
    logger.warn(
      `[${this.source.id}->${this.target.id}] Resuming from halt: ${this.haltReason}`,
    );
    this.halted = false;
    this.haltReason = null;
    this.haltedAt = null;
    this.deferred = [];
    return true;
  }

  async processBatch() {
    if (this.halted) return false;
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

      if (changes.length === 0 && this.deferred.length === 0) return false;

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

      const lastBatchId =
        changes.length > 0
          ? changes[changes.length - 1].change_id
          : lastId.toString();

      if (validEventCount === 0 && this.deferred.length === 0) {
        if (changes.length > 0) await this.setCheckpoint(lastBatchId);
        return changes.length === CONFIG.batchSize;
      }

      // Derive dynamic FK-safe order for batch + deferred
      const allTables = new Set([
        ...byTable.keys(),
        ...this.deferred.map((d) => d.table),
      ]);
      let tableOrder;
      try {
        const result = await getDependencyOrder(this.target, [...allTables]);
        tableOrder = result.order;
      } catch (err) {
        logger.error(
          `[Sync Engine] ${this.source.id} -> ${this.target.id} | ` +
            `FATAL: cannot determine FK order: ${err.message}. Skipping batch.`,
        );
        throw err;
      }

      let appliedCount = 0;
      let deferredAdded = 0;
      let deferredResolved = 0;

      try {
        await this.target.db.transaction(async (trx) => {
          let restoreSession = async () => {};
          try {
            restoreSession = await this.setSessionBypass(trx);

            // Phase A: retry deferred
            if (this.deferred.length > 0) {
              const r = await this.retryDeferred(trx);
              deferredResolved = r.resolved;
              this.deferred = r.stillDeferred;
            }

            // Phase B: apply new work in FK-safe order
            for (const tableName of tableOrder) {
              const tableChanges = byTable.get(tableName);
              if (!tableChanges || tableChanges.length === 0) continue;

              try {
                await this.applyTableChanges(trx, tableName, tableChanges);
                appliedCount += tableChanges.length;
              } catch (tableErr) {
                logger.error(
                  `[Sync Engine] Table apply failed for ${tableName}: ${tableErr.message}. Per-row fallback.`,
                );
                for (const change of tableChanges) {
                  try {
                    await this.applyTableChanges(trx, tableName, [change]);
                    appliedCount++;
                  } catch (rowErr) {
                    if (this.isDeferrableFkError(rowErr)) {
                      if (this.deferred.length >= CONFIG.deferMaxQueueSize) {
                        this.halted = true;
                        this.haltReason = `Deferred queue full (${CONFIG.deferMaxQueueSize})`;
                        this.haltedAt = new Date();
                        logger.error(
                          `[HALT] ${this.source.id}->${this.target.id}: ${this.haltReason}`,
                        );
                        throw new Error(`Sync halted: ${this.haltReason}`);
                      }
                      this.deferred.push({
                        table: tableName,
                        change,
                        attempts: 1,
                      });
                      deferredAdded++;
                      logger.warn(
                        `[Deferred] change_id=${change.change_id} table=${tableName} — FK not satisfied, will retry (queue=${this.deferred.length})`,
                      );
                    } else {
                      // Non-FK error: halt
                      this.halted = true;
                      this.haltReason = `change_id=${change.change_id} table=${tableName}: ${rowErr.message}`;
                      this.haltedAt = new Date();
                      logger.error(
                        `[HALT] ${this.source.id}->${this.target.id}: ${this.haltReason}`,
                      );
                      throw new Error(`Sync halted: ${this.haltReason}`);
                    }
                  }
                }
              }
            }

            // Checkpoint: do NOT advance past earliest deferred row
            const deferredIds = this.deferred
              .map((d) => BigInt(d.change.change_id))
              .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
            const checkpointValue =
              deferredIds.length > 0
                ? (deferredIds[0] - 1n).toString()
                : lastBatchId;
            await this.setCheckpoint(checkpointValue, trx);
          } finally {
            try {
              await restoreSession();
            } catch (e) {
              logger.debug(`[SessionBypass] finally: ${e.message}`);
            }
          }
        });
      } catch (batchErr) {
        logger.error(`[Sync Engine] Batch aborted: ${batchErr.message}`);
        return false;
      }

      this.stats.appliedEvents += appliedCount;
      this.stats.deferredEvents += deferredAdded;
      this.stats.resolvedDeferred += deferredResolved;
      this.stats.batches++;
      this.stats.lastAppliedAt = new Date();

      logger.info(
        `[Sync Engine] ${this.source.id} -> ${this.target.id} | Applied ${appliedCount}, ` +
          `skipped ${skippedEventCount}, deferred +${deferredAdded}/-${deferredResolved} (queue=${this.deferred.length})`,
      );

      return (
        changes.length === CONFIG.batchSize ||
        (this.deferred.length > 0 && deferredResolved > 0)
      );
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
      hubToBranch: {
        ...this.hubToBranch.stats,
        halted: this.hubToBranch.halted,
        haltReason: this.hubToBranch.haltReason,
        haltAt: this.hubToBranch.haltedAt,
        deferred: this.hubToBranch.deferred.length,
      },
      branchToHub: {
        ...this.branchToHub.stats,
        halted: this.branchToHub.halted,
        haltReason: this.branchToHub.haltReason,
        haltAt: this.branchToHub.haltedAt,
        deferred: this.branchToHub.deferred.length,
      },
    };
  }
}

/* ------------------------------------------------------------------ */
/* Startup flush                                                      */
/* ------------------------------------------------------------------ */

async function flushPendingSyncs(workers) {
  logger.info("[Startup Sync] Draining pending backlog...");
  let batchCount = 0;
  let hasMoreData = true;

  while (hasMoreData) {
    if (batchCount >= CONFIG.flushMaxIterations) {
      logger.warn(
        `[Startup Sync] Reached FLUSH_MAX_ITERATIONS (${CONFIG.flushMaxIterations})`,
      );
      break;
    }
    const tasks = workers.flatMap((w) => [
      w.hubToBranch.processBatch().catch((err) => {
        logger.error(`[Flush Error] ${w.branchId} hub->branch: ${err.message}`);
        return false;
      }),
      w.branchToHub.processBatch().catch((err) => {
        logger.error(`[Flush Error] ${w.branchId} branch->hub: ${err.message}`);
        return false;
      }),
    ]);
    const results = await Promise.all(tasks);
    hasMoreData = results.some((h) => h === true);
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
    logger.warn(`[Auth] Rejected ${req.path} from ${req.ip}`);
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

let syncManagers = [];
let managerById = new Map();

// Row-count cache for /health (approximate)
let rowCountCache = { ts: 0, hub: null, branches: new Map() };

async function approximateRowCount(node) {
  try {
    const res = await node.db.raw(
      `SELECT SUM(TABLE_ROWS) AS total
       FROM information_schema.TABLES
       WHERE TABLE_SCHEMA = DATABASE()
         AND TABLE_NAME IN (?)`,
      [[...ALLOWED_TABLES]],
    );
    const row = (res[0] || res)[0];
    return Number(row?.total || 0);
  } catch {
    return null;
  }
}

async function refreshRowCounts() {
  if (Date.now() - rowCountCache.ts < CONFIG.healthCacheTtlMs)
    return rowCountCache;
  const hub = await approximateRowCount(HUB_NODE);
  const branches = new Map();
  await Promise.all(
    branchNodes.map(async (b) => {
      branches.set(b.id, await approximateRowCount(b));
    }),
  );
  rowCountCache = { ts: Date.now(), hub, branches };
  return rowCountCache;
}

app.get("/health", requireApiKey, async (req, res) => {
  let hubMaxChangeId = null;
  try {
    const row = await HUB_NODE.db("sym_change_log")
      .max("change_id as m")
      .first();
    hubMaxChangeId = row?.m != null ? BigInt(row.m) : 0n;
  } catch (err) {
    logger.error(`[Health] hub max change_id failed: ${err.message}`);
  }

  const counts = await refreshRowCounts();

  const branchStatus = await Promise.all(
    branchNodes.map(async (branch) => {
      const manager = managerById.get(branch.id);
      const h2b = manager?.hubToBranch;
      const b2h = manager?.branchToHub;
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

        const hubToBranchLag =
          hubMaxChangeId != null ? hubMaxChangeId - lastProcessed : null;
        const branchToHubLag = null; // could compute similar way

        const isHalted = h2b?.halted || b2h?.halted;

        return {
          branchId: branch.id,
          status: "CONNECTED",
          lastProcessedChangeId: lastProcessed.toString(),
          hubToBranchLag: lag,
          rowCounts: {
            hub: counts.hub,
            branch: counts.branches.get(branch.id),
          },
          deferredQueueDepth: {
            hubToBranch: h2b?.deferred.length ?? 0,
            branchToHub: b2h?.deferred.length ?? 0,
          },
          halted: {
            hubToBranch: h2b?.halted ?? false,
            branchToHub: b2h?.halted ?? false,
            reason: h2b?.haltReason || b2h?.haltReason || null,
            at: h2b?.haltedAt || b2h?.haltedAt || null,
          },
          healthy: manager
            ? manager.consecutiveErrors === 0 && !isHalted
            : false,
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
        };
      }
    }),
  );

  const allHealthy = branchStatus.every((b) => b.healthy !== false);
  const anyHalted = branchStatus.some(
    (b) => b.halted?.hubToBranch || b.halted?.branchToHub,
  );
  const totalDeferred = branchStatus.reduce(
    (s, b) =>
      s +
      (b.deferredQueueDepth?.hubToBranch || 0) +
      (b.deferredQueueDepth?.branchToHub || 0),
    0,
  );

  res.json({
    hub: HUB_NODE.id,
    healthy: allHealthy && !anyHalted,
    anyHalted,
    activeBranches: branchStatus.length,
    totalDeferredRows: totalDeferred,
    branches: branchStatus,
  });
});

app.post("/resume/:branchId", requireApiKey, (req, res) => {
  const manager = managerById.get(req.params.branchId);
  if (!manager) {
    return res.status(404).json({ error: "branch not found" });
  }
  const hubResumed = manager.hubToBranch.resume();
  const branchResumed = manager.branchToHub.resume();
  logger.warn(
    `[Resume] Branch [${req.params.branchId}] hub->branch=${hubResumed} branch->hub=${branchResumed}`,
  );
  res.json({
    branchId: req.params.branchId,
    resumed: { hubToBranch: hubResumed, branchToHub: branchResumed },
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
        `encryptionEnabled=${Boolean(CONFIG.encryptionKey)} ` +
        `deferMaxAttempts=${CONFIG.deferMaxAttempts}`,
    );

    await provisionNode(HUB_NODE);

    const flushList = [];

    for (const branch of branchNodes) {
      try {
        await provisionNode(branch);
        logger.info(`[Startup] Provisioned branch [${branch.id}]`);
      } catch (err) {
        logger.warn(
          `[Startup] Branch [${branch.id}] provisioning failed: ${err.message}`,
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
        logger.error(`[Fatal Branch Loop Error] ${m.branchId}: ${err.stack}`);
      });
    });

    logger.info(`All ${syncManagers.length} branch sync loops launched.`);
  } catch (err) {
    logger.error(`[Fatal Hub Engine Error]: ${err.stack}`);
    process.exit(1);
  }
}

async function shutdown(signal) {
  if (isShuttingDown) {
    logger.info("Forced shutdown requested.");
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
        `Error closing branch [${branchNodes[i].id}]: ${r.reason?.message}`,
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
