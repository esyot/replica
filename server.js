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
  verifyNodeTriggers,
  getSyncTokenForNode,
  ALLOWED_TABLES,
} = require("./triggers");
const { decodeCompositeKey } = require("./pk-codec");

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

function requireEnv(name) {
  const v = process.env[name];
  if (!v) {
    logger.error(`CRITICAL: ${name} missing in environment!`);
    process.exit(1);
  }
  return v;
}
function parseIntEnv(name, dv, { min, max } = {}) {
  const raw = process.env[name];
  let v = raw !== undefined && raw !== "" ? parseInt(raw, 10) : dv;
  if (Number.isNaN(v)) {
    if (raw !== undefined && raw !== "")
      logger.warn(`Invalid integer ${name}="${raw}", using ${dv}`);
    v = dv;
  }
  if (min !== undefined) v = Math.max(min, v);
  if (max !== undefined) v = Math.min(max, v);
  return v;
}
function parseBoolEnv(name, dv = false) {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return dv;
  return raw === "true" || raw === "1";
}

const API_SECRET_KEY = requireEnv("API_SECRET_KEY");
const API_SECRET_KEY_HASH = crypto
  .createHash("sha256")
  .update(API_SECRET_KEY)
  .digest();

let ALLOWED_COLUMNS = {};
try {
  ALLOWED_COLUMNS = JSON.parse(process.env.ALLOWED_COLUMNS_JSON || "{}");
} catch (err) {
  logger.error(`Invalid ALLOWED_COLUMNS_JSON: ${err.message}`);
}
for (const t of ALLOWED_TABLES) {
  if (!Array.isArray(ALLOWED_COLUMNS[t]))
    logger.warn(
      `[Config] ALLOWED_COLUMNS missing for "${t}" — all columns accepted.`,
    );
}

const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((o) => o.trim())
  .filter(Boolean);

function parseConnectionString(urlStr, defaultPort = 3306) {
  if (!urlStr) throw new Error("parseConnectionString: empty");
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
function normalizeClient(c) {
  if (!c) return "mysql2";
  const x = c.toLowerCase().trim();
  if (x === "mysql") return "mysql2";
  if (x === "postgresql") return "pg";
  return x;
}
function isPgClient(c) {
  return c === "pg" || c === "postgres";
}
function isMysqlClient(c) {
  return c === "mysql2" || c === "mysql";
}
function defaultPortFor(c) {
  return isPgClient(c) ? 5432 : 3306;
}
function buildKnexConnection(client, opts) {
  if (isPgClient(client))
    return {
      connectionString: opts.url,
      ssl: opts.ssl ? { rejectUnauthorized: opts.sslStrict !== false } : false,
    };
  const conn = parseConnectionString(opts.url, defaultPortFor(client));
  if (opts.ssl) conn.ssl = { rejectUnauthorized: opts.sslStrict !== false };
  return conn;
}

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

let BRANCHES_CONFIG = [];
try {
  BRANCHES_CONFIG = JSON.parse(process.env.BRANCHES_JSON || "[]");
  if (!Array.isArray(BRANCHES_CONFIG))
    throw new Error("BRANCHES_JSON must be an array");
} catch (err) {
  logger.error(`CRITICAL: Invalid BRANCHES_JSON: ${err.message}`);
  process.exit(1);
}
for (const b of BRANCHES_CONFIG)
  if (!b.id || !b.url) {
    logger.error(
      `CRITICAL: BRANCHES_JSON entry needs id+url: ${JSON.stringify(b)}`,
    );
    process.exit(1);
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
        ssl: b.ssl === true,
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
  deferMaxAttempts: parseIntEnv("DEFER_MAX_ATTEMPTS", 5, { min: 1, max: 100 }),
  deferMaxQueueSize: parseIntEnv("DEFER_MAX_QUEUE_SIZE", 10000, { min: 1 }),
  dependencyOrderTtlMs: parseIntEnv("DEPENDENCY_ORDER_TTL_MS", 60000, {
    min: 1000,
  }),
  healthCacheTtlMs: parseIntEnv("HEALTH_CACHE_TTL_MS", 30000, { min: 1000 }),
  relayEnabled: parseBoolEnv("RELAY_ENABLED", true),
  failOnMissingTriggers: parseBoolEnv("FAIL_ON_MISSING_TRIGGERS", true),
  expectedTriggersPerTable: parseIntEnv("EXPECTED_TRIGGERS_PER_TABLE", 3, {
    min: 1,
    max: 10,
  }),
  logFidelityIntervalMs: parseIntEnv("LOG_FIDELITY_INTERVAL_MS", 120000, {
    min: 30000,
  }),
  logFidelityAlertLagMs: parseIntEnv("LOG_FIDELITY_ALERT_LAG_MS", 300000, {
    min: 60000,
  }),
  logFidelityWebhookUrl: process.env.LOG_FIDELITY_WEBHOOK_URL || null,
  // post-batch verification
  verifyAfterApply: parseBoolEnv("VERIFY_AFTER_APPLY", true),
};

if (CONFIG.encryptionKey) {
  let k;
  try {
    k = Buffer.from(CONFIG.encryptionKey, "hex");
  } catch {
    k = Buffer.alloc(0);
  }
  if (k.length !== 32) {
    logger.error("CRITICAL: PAYLOAD_ENCRYPTION_KEY must be 32 bytes (64 hex).");
    process.exit(1);
  }
}

class PayloadCipher {
  static decrypt(ct, keyHex) {
    if (!keyHex || ct == null) return ct;
    let parsed;
    try {
      parsed = typeof ct === "string" ? JSON.parse(ct) : ct;
    } catch {
      throw new Error("Payload is not a valid encrypted envelope");
    }
    if (!parsed || !parsed.iv || !parsed.encrypted || !parsed.authTag)
      throw new Error("Payload is plaintext but PAYLOAD_ENCRYPTION_KEY is set");
    const d = crypto.createDecipheriv(
      "aes-256-gcm",
      Buffer.from(keyHex, "hex"),
      Buffer.from(parsed.iv, "hex"),
    );
    d.setAuthTag(Buffer.from(parsed.authTag, "hex"));
    let out = d.update(parsed.encrypted, "hex", "utf8");
    out += d.final("utf8");
    return out;
  }
}

async function provisionNode(node) {
  const db = node.db,
    client = node.client;
  if (isMysqlClient(client)) {
    try {
      await db.raw("SET GLOBAL log_bin_trust_function_creators = 1;");
    } catch (err) {
      logger.debug(
        `[Provision] Skip log_bin_trust on ${node.id}: ${err.message}`,
      );
    }
  }
  if (!(await db.schema.hasTable("sym_change_log"))) {
    await db.schema.createTable("sym_change_log", (t) => {
      t.bigIncrements("change_id").primary();
      t.string("table_name", 100).notNullable();
      t.string("operation", 10).notNullable();
      t.string("primary_key_val", 255).notNullable();
      t.text("row_data").notNullable();
      t.string("node_source_id", 50).notNullable();
      t.timestamp("created_at").defaultTo(db.fn.now());
    });
    logger.info(`[Provision] Created sym_change_log on [${node.id}]`);
  }
  if (!(await db.schema.hasTable("sym_checkpoint"))) {
    await db.schema.createTable("sym_checkpoint", (t) => {
      t.string("node_id", 50).primary();
      t.bigInteger("last_processed_change_id").notNullable();
      t.timestamp("updated_at").defaultTo(db.fn.now());
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
    logger.warn(
      `[Provision] SETUP_TRIGGERS_ON_STARTUP=true — recreating triggers on ${node.id}. Set false in production.`,
    );
    await setupNodeTriggers(node);
  }
}

const PK_CACHE = new Map();
async function getCachedPkColumns(node, tableName) {
  if (!ALLOWED_TABLES.has(tableName))
    throw new Error(`Refusing PK for disallowed table: ${tableName}`);
  const cacheKey = `${node.id}:${tableName}`;
  if (!PK_CACHE.has(cacheKey)) {
    const p = getTablePkColumns(node, tableName).then((cols) => {
      if (!cols || !cols.length) throw new Error(`No PK for ${tableName}`);
      return cols;
    });
    p.catch(() => PK_CACHE.delete(cacheKey));
    PK_CACHE.set(cacheKey, p);
  }
  return PK_CACHE.get(cacheKey);
}

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
  const db = node.db,
    client = node.client;
  let rawEdges = [];
  try {
    if (isMysqlClient(client)) {
      const res = await db.raw(
        `SELECT TABLE_NAME AS child, REFERENCED_TABLE_NAME AS parent FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL`,
      );
      rawEdges = (res[0] || res).map((r) => ({
        child: r.child,
        parent: r.parent,
      }));
    } else if (isPgClient(client)) {
      const res = await db.raw(
        `SELECT kcu.table_name AS child, ccu.table_name AS parent FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = current_schema()`,
      );
      rawEdges = (res.rows || []).map((r) => ({
        child: r.child,
        parent: r.parent,
      }));
    }
  } catch (err) {
    logger.warn(
      `[table-order] FK query failed on ${node.id}: ${err.message} — alphabetical fallback`,
    );
    return { order: [...tables].sort(), edges: new Map() };
  }
  const inBatch = new Set(tables);
  const parentsOf = new Map();
  for (const t of tables) parentsOf.set(t, new Set());
  for (const e of rawEdges)
    if (inBatch.has(e.child) && inBatch.has(e.parent))
      parentsOf.get(e.child).add(e.parent);
  const visited = new Set(),
    visiting = new Set(),
    order = [],
    cyclePaths = [];
  function visit(t, stack) {
    if (visited.has(t)) return;
    if (visiting.has(t)) {
      const i = stack.indexOf(t);
      if (i >= 0) cyclePaths.push(stack.slice(i).concat([t]));
      return;
    }
    visiting.add(t);
    stack.push(t);
    for (const p of parentsOf.get(t) || []) visit(p, stack);
    stack.pop();
    visiting.delete(t);
    visited.add(t);
    order.push(t);
  }
  for (const t of tables) visit(t, []);
  const cycles = cyclePaths.filter((c) => new Set(c).size > 1);
  if (cycles.length)
    throw new Error(
      `[table-order] FK cycle on ${node.id}: ${cycles.map((c) => c.join(" -> ")).join("; ")}`,
    );
  for (const t of tables) if (!visited.has(t)) order.push(t);
  DEP_ORDER_CACHE.set(cacheKey, { order, edges: parentsOf, ts: Date.now() });
  return { order, edges: parentsOf };
}
function clearDependencyOrderCache(nodeId) {
  if (!nodeId) DEP_ORDER_CACHE.clear();
  else DEP_ORDER_CACHE.delete(nodeId);
}

async function verifyAllTriggers(nodes) {
  const problems = [];
  for (const n of nodes) {
    const isPg = isPgClient(n.client);
    const expected = isPg ? 1 : CONFIG.expectedTriggersPerTable;
    const v = await verifyNodeTriggers(n, expected);
    if (v.error) {
      problems.push({ node: n.id, error: v.error });
      logger.error(`[TriggerVerify] ${n.id}: ${v.error}`);
    } else if (!v.ok) {
      const sample = v.missing
        .slice(0, 10)
        .map((m) => `${m.table}(${m.have}/${m.need})`)
        .join(", ");
      logger.error(
        `[TriggerVerify] ${n.id}: ${v.missing.length} table(s) short of ${expected}: ${sample}${v.missing.length > 10 ? ", …" : ""}`,
      );
      problems.push({ node: n.id, missing: v.missing });
    } else
      logger.info(
        `[TriggerVerify] ${n.id}: OK (${ALLOWED_TABLES.size} tables × ${expected})`,
      );
  }
  return problems;
}

// ---------------------------------------------------------------- log fidelity
//
// Sync-applied writes touch target tables without writing a log entry
// (triggers suppressed via @sym_sync_token). So a naive "table newer than log"
// check fires on every sync target permanently.
//
// Correct check: only alert if the target table's UPDATE_TIME is newer than
// the newest event the target's own checkpoint has processed. If the target
// is caught up on the log, table-write-time <= log-max is a false positive
// from sync applies.

async function logFidelityCheck(node) {
  const results = [];
  // Grace period so a check running right after a sync batch doesn't fire.
  const GRACE_MS = 120_000;
  const now = Date.now();

  for (const t of ALLOWED_TABLES) {
    try {
      const r = await node.db.raw(
        `SELECT UPDATE_TIME AS u FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1`,
        [t],
      );
      const u = (r[0] || r)[0]?.u;
      if (!u) continue;
      const updateTime = new Date(u).getTime();
      if (now - updateTime < GRACE_MS) continue; // freshly applied, ignore

      // Newest event this node has applied from ANY source (i.e., its
      // inbound checkpoints). If the table's write time is newer than that,
      // and it's not within grace, it was a local write that didn't log.
      const cpMax = await node
        .db("sym_checkpoint")
        .max("updated_at as m")
        .first();
      const lastCpAt = cpMax?.m ? new Date(cpMax.m).getTime() : 0;
      if (updateTime <= lastCpAt) continue; // table touched by an apply, not local

      // Also: if the table's own log has an entry within the grace window,
      // it's a legitimate local write — skip.
      const ownLog = await node
        .db("sym_change_log")
        .where("table_name", t)
        .where("created_at", ">=", new Date(updateTime - GRACE_MS))
        .max("created_at as m")
        .first();
      if (ownLog?.m) continue;

      const lag = now - updateTime;
      if (lag > CONFIG.logFidelityAlertLagMs)
        results.push({ table: t, updateTime: u, lagMs: lag });
    } catch (err) {
      logger.debug(`[LogFidelity] ${node.id}/${t}: ${err.message}`);
    }
  }

  if (results.length) {
    const msg = `[LogFidelity] ${node.id}: ${results.length} table(s) likely written outside the log. Sample: ${results
      .slice(0, 5)
      .map((r) => `${r.table}(lag=${Math.round(r.lagMs / 1000)}s)`)
      .join(", ")}`;
    logger.error(msg);
    if (CONFIG.logFidelityWebhookUrl && typeof fetch === "function") {
      try {
        fetch(CONFIG.logFidelityWebhookUrl, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            node: node.id,
            message: msg,
            tables: results,
          }),
        }).catch(() => {});
      } catch {}
    }
  }
  return results;
}

// ---------------------------------------------------------------- sync worker

class SyncWorker {
  constructor(source, target, opts = {}) {
    this.source = source;
    this.target = target;
    this.isProcessing = false;
    this.checkpointCallCount = 0;
    this.isHub = target.id === HUB_NODE.id;
    this.relayEnabled = this.isHub && CONFIG.relayEnabled && !opts.disableRelay;
    this.deferred = [];
    this.halted = false;
    this.haltReason = null;
    this.haltedAt = null;
    this.stats = {
      appliedEvents: 0,
      skippedEvents: 0,
      relayedEvents: 0,
      quarantinedEvents: 0,
      deferredEvents: 0,
      resolvedDeferred: 0,
      batches: 0,
      verifiedOk: 0,
      verifiedFail: 0,
      lastAppliedAt: null,
    };
  }
  async getCheckpoint() {
    const rec = await this.target
      .db("sym_checkpoint")
      .where("node_id", this.source.id)
      .first();
    let cp = rec ? BigInt(rec.last_processed_change_id) : 0n;
    this.checkpointCallCount++;
    if (this.checkpointCallCount % CONFIG.checkpointValidationEvery !== 0)
      return cp;
    const r = await this.source
      .db("sym_change_log")
      .max("change_id as max_id")
      .first();
    const maxSrc = r?.max_id != null ? BigInt(r.max_id) : 0n;
    if (cp > maxSrc) {
      logger.warn(
        `[Checkpoint Resync] ${this.source.id}->${this.target.id}: ${cp} > ${maxSrc}, resetting.`,
      );
      cp = maxSrc;
      await this.setCheckpoint(cp);
    }
    return cp;
  }
  async setCheckpoint(id, trx) {
    const db = trx || this.target.db;
    await db("sym_checkpoint")
      .insert({
        node_id: this.source.id,
        last_processed_change_id: id.toString(),
        updated_at: new Date(),
      })
      .onConflict("node_id")
      .merge();
  }
  async setSessionBypass(trx) {
    const client = this.target.client;
    if (isPgClient(client)) {
      const token = getSyncTokenForNode(this.target.id);
      await trx.raw("SELECT set_config('sym.sync_token', ?, true)", [token]);
      return async () => {};
    }
    if (isMysqlClient(client)) {
      const token = getSyncTokenForNode(this.target.id);
      const restore = async () => {
        try {
          await trx.raw("SET FOREIGN_KEY_CHECKS = 1;");
        } catch {}
        try {
          await trx.raw("SET @sym_sync_token = NULL;");
        } catch {}
      };
      try {
        await trx.raw("SET @sym_sync_token = ?", [token]);
        await trx.raw("SET FOREIGN_KEY_CHECKS = 0;");
        await trx.raw(
          "SET SESSION sql_mode = REPLACE(REPLACE(@@sql_mode, 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '');",
        );
      } catch (e) {
        await restore();
        throw e;
      }
      return restore;
    }
    return async () => {};
  }
  decodePayload(change) {
    let raw = change.row_data;
    if (CONFIG.encryptionKey)
      raw = PayloadCipher.decrypt(change.row_data, CONFIG.encryptionKey);
    let p;
    if (typeof raw === "string") {
      try {
        p = JSON.parse(raw);
      } catch {
        p = raw;
      }
    } else p = raw;
    if (!p || typeof p !== "object") return p;
    const allowed = ALLOWED_COLUMNS[change.table_name];
    const keys = Array.isArray(allowed) ? allowed : Object.keys(p);
    const out = {};
    for (const k of keys)
      if (Object.prototype.hasOwnProperty.call(p, k)) {
        let v = p[k];
        if (typeof v === "string" && /^0000-00-00(?: 00:00:00)?$/.test(v))
          v = null;
        out[k] = v;
      }
    return out;
  }
  async bulkDeleteComposite(trx, table, pkCols, deletes) {
    const chunk = CONFIG.compositeDeleteChunkSize;
    for (let i = 0; i < deletes.length; i += chunk) {
      const slice = deletes.slice(i, i + chunk);
      const b = trx(table);
      b.where(function () {
        for (const c of slice) {
          const v = decodeCompositeKey(c.primary_key_val, pkCols.length, table);
          this.orWhere(function () {
            pkCols.forEach((col, idx) => this.andWhere(col, v[idx]));
          });
        }
      });
      await b.del();
    }
  }

  // After every batch, verify that every row we intended to write actually
  // landed on target. If any failed (silent skip due to FK orphan or other
  // constraint), halt. This closes the "checkpoint advances past unapplied
  // events" hole.
  //
  // FIX B: this now takes the batch's transaction (`trx`) and queries through
  // it. Previously it queried `this.target.db`, which uses a different pooled
  // connection and cannot see the batch's own uncommitted writes — so every
  // DELETE looked like it "did not land" and every new INSERT looked missing,
  // halting the worker on the first batch containing either.
  async verifyBatchApplied(trx, table, tableChanges) {
    const pkCols = await getCachedPkColumns(this.target, table);
    if (pkCols.length !== 1) return { ok: true, skipped: "composite-pk" };
    const pkCol = pkCols[0];

    const inserts = [];
    const deletes = [];
    for (const c of tableChanges) {
      if (c.operation === "DELETE") deletes.push(c.primary_key_val);
      else inserts.push(c.primary_key_val);
    }

    const bad = [];
    const CH = 500;

    if (inserts.length) {
      const existing = new Set();
      for (let i = 0; i < inserts.length; i += CH) {
        const slice = inserts.slice(i, i + CH);
        const rows = await trx(table).select(pkCol).whereIn(pkCol, slice);
        for (const r of rows) existing.add(String(r[pkCol]));
      }
      for (const id of inserts) {
        if (!existing.has(String(id))) bad.push({ op: "INSERT", pk: id });
      }
    }

    if (deletes.length) {
      const still = new Set();
      for (let i = 0; i < deletes.length; i += CH) {
        const slice = deletes.slice(i, i + CH);
        const rows = await trx(table).select(pkCol).whereIn(pkCol, slice);
        for (const r of rows) still.add(String(r[pkCol]));
      }
      for (const id of deletes) {
        if (still.has(String(id))) bad.push({ op: "DELETE", pk: id });
      }
    }

    if (bad.length) {
      this.stats.verifiedFail += bad.length;
      const sample = bad
        .slice(0, 5)
        .map((b) => `${b.op}:${b.pk}`)
        .join(", ");
      this.halted = true;
      this.haltReason = `post-apply verify failed for ${table}: ${bad.length} row(s) did not land. Sample: ${sample}. Checkpoint NOT advanced. Resume after manual repair.`;
      this.haltedAt = new Date();
      logger.error(
        `[HALT/VERIFY] ${this.source.id}->${this.target.id}: ${this.haltReason}`,
      );
      throw new Error(`Sync halted: ${this.haltReason}`);
    }
    this.stats.verifiedOk += inserts.length + deletes.length;
    return { ok: true, checked: inserts.length + deletes.length };
  }

  async applyTableChanges(trx, table, tableChanges) {
    const pkCols = await getCachedPkColumns(this.target, table);
    const deletes = [],
      upserts = [];
    for (const c of tableChanges) {
      if (c.operation === "DELETE") deletes.push(c);
      else upserts.push(this.decodePayload(c));
    }
    if (deletes.length) {
      if (pkCols.length === 1) {
        const col = pkCols[0];
        const ids = deletes.map((d) => d.primary_key_val);
        const sz = CONFIG.upsertChunkSize;
        for (let i = 0; i < ids.length; i += sz)
          await trx(table)
            .whereIn(col, ids.slice(i, i + sz))
            .del();
      } else await this.bulkDeleteComposite(trx, table, pkCols, deletes);
    }
    if (upserts.length) {
      const sz = CONFIG.upsertChunkSize;
      for (let i = 0; i < upserts.length; i += sz)
        await trx(table)
          .insert(upserts.slice(i, i + sz))
          .onConflict(pkCols)
          .merge();
    }
  }
  async relayChanges(trx, table, tableChanges) {
    if (!this.relayEnabled || !tableChanges.length) return 0;
    const rows = tableChanges.map((c) => ({
      table_name: c.table_name,
      operation: c.operation,
      primary_key_val: c.primary_key_val,
      row_data: c.row_data,
      node_source_id: this.target.id,
    }));
    let ins = 0;
    const sz = CONFIG.upsertChunkSize;
    for (let i = 0; i < rows.length; i += sz) {
      const chunk = rows.slice(i, i + sz);
      try {
        await trx("sym_change_log").insert(chunk);
        ins += chunk.length;
      } catch (err) {
        logger.warn(
          `[Relay] Failed to re-emit ${chunk.length} events for ${table}: ${err.message}`,
        );
      }
    }
    return ins;
  }
  isDeferrableFkError(err) {
    const m = String(err?.message || err || "");
    return (
      /ER_NO_REFERENCED_ROW/i.test(m) ||
      /Cannot add or update a child row: a foreign key constraint fails/i.test(
        m,
      ) ||
      /foreign key constraint fails/i.test(m)
    );
  }
  async retryDeferred(trx) {
    if (!this.deferred.length) return { resolved: 0, stillDeferred: [] };
    const tables = [...new Set(this.deferred.map((d) => d.table))];
    let order;
    try {
      ({ order } = await getDependencyOrder(this.target, tables));
    } catch {
      order = tables;
    }
    const idx = new Map(order.map((t, i) => [t, i]));
    const sorted = [...this.deferred].sort(
      (a, b) => (idx.get(a.table) ?? 9999) - (idx.get(b.table) ?? 9999),
    );
    const still = [];
    let resolved = 0;
    for (const d of sorted) {
      try {
        await this.applyTableChanges(trx, d.table, [d.change]);
        resolved++;
        logger.info(`[Deferred] Resolved change_id=${d.change.change_id}`);
      } catch (err) {
        d.attempts++;
        if (d.attempts >= CONFIG.deferMaxAttempts) {
          this.halted = true;
          this.haltReason = `change_id=${d.change.change_id} table=${d.table} failed ${d.attempts}x: ${err.message}`;
          this.haltedAt = new Date();
          logger.error(
            `[HALT] ${this.source.id}->${this.target.id}: ${this.haltReason}`,
          );
          throw new Error(`Sync halted: ${this.haltReason}`);
        }
        still.push(d);
      }
    }
    return { resolved, stillDeferred: still };
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
    if (this.halted || this.isProcessing) return false;
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
      if (!changes.length && !this.deferred.length) return false;
      const byTable = new Map();
      let valid = 0,
        skipped = 0;

      // FIX A: collapse to the latest event per (table, PK).
      // `changes` is ordered by change_id ASC, so a later event overwrites an
      // earlier one. row_data holds a full-row snapshot, so last-writer-wins
      // is correct. Without this, a DELETE + INSERT (or INSERT + DELETE) of the
      // same PK in one batch gets applied as "all deletes first, then all
      // upserts", which can resurrect a deleted row or contradict the verify.
      // `lastBatchId` below is still taken from the full `changes` list, so
      // the checkpoint advance is unaffected.
      const latest = new Map();
      for (const c of changes) {
        if (!ALLOWED_TABLES.has(c.table_name)) {
          skipped++;
          continue;
        }
        latest.set(`${c.table_name}\u0000${c.primary_key_val}`, c);
      }
      for (const c of latest.values()) {
        if (!byTable.has(c.table_name)) byTable.set(c.table_name, []);
        byTable.get(c.table_name).push(c);
        valid++;
      }

      if (skipped) {
        this.stats.skippedEvents += skipped;
        logger.warn(
          `[Sync Engine] ${this.source.id}->${this.target.id} | skipped ${skipped} disallowed`,
        );
      }
      const lastBatchId = changes.length
        ? changes[changes.length - 1].change_id
        : lastId.toString();
      if (!valid && !this.deferred.length) {
        if (changes.length) await this.setCheckpoint(lastBatchId);
        return changes.length === CONFIG.batchSize;
      }
      const all = new Set([
        ...byTable.keys(),
        ...this.deferred.map((d) => d.table),
      ]);
      let tableOrder;
      try {
        ({ order: tableOrder } = await getDependencyOrder(this.target, [
          ...all,
        ]));
      } catch (err) {
        logger.error(
          `[Sync Engine] ${this.source.id}->${this.target.id} | cannot determine FK order: ${err.message}`,
        );
        throw err;
      }

      let appliedCount = 0,
        deferredAdded = 0,
        deferredResolved = 0,
        relayedCount = 0;

      // Insert-order is parent-before-child; delete-order is the reverse
      // (child-before-parent).
      const deleteReverseOrder = [...tableOrder].reverse();

      try {
        await this.target.db.transaction(async (trx) => {
          let restore = async () => {};
          try {
            restore = await this.setSessionBypass(trx);
            if (this.deferred.length) {
              const r = await this.retryDeferred(trx);
              deferredResolved = r.resolved;
              this.deferred = r.stillDeferred;
            }

            // Phase 1: DELETES, children-first.
            for (const table of deleteReverseOrder) {
              const tcs = byTable.get(table);
              if (!tcs) continue;
              const dels = tcs.filter((c) => c.operation === "DELETE");
              if (!dels.length) continue;
              const ok = [];
              try {
                await this.applyTableChanges(trx, table, dels);
                appliedCount += dels.length;
                ok.push(...dels);
              } catch (tableErr) {
                logger.error(
                  `[Sync Engine] DELETE phase failed for ${table}: ${tableErr.message}. Per-row fallback.`,
                );
                for (const c of dels) {
                  try {
                    await this.applyTableChanges(trx, table, [c]);
                    appliedCount++;
                    ok.push(c);
                  } catch (rowErr) {
                    if (this.isDeferrableFkError(rowErr)) {
                      if (this.deferred.length >= CONFIG.deferMaxQueueSize) {
                        this.halted = true;
                        this.haltReason = `Deferred queue full`;
                        this.haltedAt = new Date();
                        throw new Error(`Sync halted: ${this.haltReason}`);
                      }
                      this.deferred.push({ table, change: c, attempts: 1 });
                      deferredAdded++;
                    } else {
                      this.halted = true;
                      this.haltReason = `change_id=${c.change_id} table=${table}: ${rowErr.message}`;
                      this.haltedAt = new Date();
                      logger.error(
                        `[HALT] ${this.source.id}->${this.target.id}: ${this.haltReason}`,
                      );
                      throw new Error(`Sync halted: ${this.haltReason}`);
                    }
                  }
                }
              }
              if (ok.length && this.relayEnabled)
                relayedCount += await this.relayChanges(trx, table, ok);
            }

            // Phase 2: INSERTS/UPDATES, parents-first (original tableOrder).
            for (const table of tableOrder) {
              const tcs = byTable.get(table);
              if (!tcs) continue;
              const up = tcs.filter((c) => c.operation !== "DELETE");
              if (!up.length) continue;
              const ok = [];
              try {
                await this.applyTableChanges(trx, table, up);
                appliedCount += up.length;
                ok.push(...up);
              } catch (tableErr) {
                logger.error(
                  `[Sync Engine] UPDATE/INSERT phase failed for ${table}: ${tableErr.message}. Per-row fallback.`,
                );
                for (const c of up) {
                  try {
                    await this.applyTableChanges(trx, table, [c]);
                    appliedCount++;
                    ok.push(c);
                  } catch (rowErr) {
                    if (this.isDeferrableFkError(rowErr)) {
                      if (this.deferred.length >= CONFIG.deferMaxQueueSize) {
                        this.halted = true;
                        this.haltReason = `Deferred queue full`;
                        this.haltedAt = new Date();
                        throw new Error(`Sync halted: ${this.haltReason}`);
                      }
                      this.deferred.push({ table, change: c, attempts: 1 });
                      deferredAdded++;
                    } else {
                      this.halted = true;
                      this.haltReason = `change_id=${c.change_id} table=${table}: ${rowErr.message}`;
                      this.haltedAt = new Date();
                      logger.error(
                        `[HALT] ${this.source.id}->${this.target.id}: ${this.haltReason}`,
                      );
                      throw new Error(`Sync halted: ${this.haltReason}`);
                    }
                  }
                }
              }
              if (ok.length && this.relayEnabled)
                relayedCount += await this.relayChanges(trx, table, ok);
            }

            // Don't advance the checkpoint unless every applied row actually
            // landed. Runs AFTER all applies, BEFORE setCheckpoint. Throws on
            // mismatch → transaction rolls back, checkpoint stays at its
            // pre-batch value, worker halts.
            // FIX B: pass `trx` so the verify sees this batch's own writes.
            if (CONFIG.verifyAfterApply) {
              for (const table of tableOrder) {
                const tcs = byTable.get(table);
                if (!tcs) continue;
                const relevant = tcs.filter(
                  (c) =>
                    !this.deferred.some(
                      (d) => d.change.change_id === c.change_id,
                    ),
                );
                if (relevant.length)
                  await this.verifyBatchApplied(trx, table, relevant);
              }
            }

            const dIds = this.deferred
              .map((d) => BigInt(d.change.change_id))
              .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
            const cp = dIds.length ? (dIds[0] - 1n).toString() : lastBatchId;
            await this.setCheckpoint(cp, trx);
          } finally {
            try {
              await restore();
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
      this.stats.relayedEvents += relayedCount;
      this.stats.deferredEvents += deferredAdded;
      this.stats.resolvedDeferred += deferredResolved;
      this.stats.batches++;
      this.stats.lastAppliedAt = new Date();
      logger.info(
        `[Sync Engine] ${this.source.id} -> ${this.target.id} | Applied ${appliedCount}, relayed ${relayedCount}, skipped ${skipped}, deferred +${deferredAdded}/-${deferredResolved} (queue=${this.deferred.length})`,
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

class BranchSyncManager {
  constructor(id, h2b, b2h, opts = {}) {
    this.branchId = id;
    this.hubToBranch = h2b;
    this.branchToHub = b2h;
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
      let hadWork = false,
        hadError = false;
      try {
        const [hm, bm] = await Promise.all([
          this.hubToBranch.processBatch(),
          this.branchToHub.processBatch(),
        ]);
        hadWork = hm || bm;
        if (this.consecutiveErrors > 0)
          logger.info(
            `[${this.branchId}] recovered after ${this.consecutiveErrors} errors`,
          );
        this.consecutiveErrors = 0;
        this.lastError = null;
      } catch (err) {
        hadError = true;
        this.consecutiveErrors++;
        this.lastError = err.message;
        this.lastErrorAt = new Date();
        logger.error(
          `[${this.branchId}] sync error #${this.consecutiveErrors}: ${err.stack || err.message}`,
        );
      }
      this.lastRunAt = new Date();
      if (!this.running) break;
      if (hadWork && !hadError) {
        await new Promise((r) => setImmediate(r));
        continue;
      }
      const base = hadError
        ? Math.min(
            this.pollIntervalMs * 2 ** Math.min(this.consecutiveErrors, 6),
            this.maxBackoffMs,
          )
        : this.pollIntervalMs;
      const delay = hadError ? base * (0.85 + Math.random() * 0.3) : base;
      await new Promise((r) => setTimeout(r, delay));
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

async function flushPendingSyncs(workers) {
  logger.info("[Startup Sync] Draining backlog...");
  let n = 0,
    more = true;
  while (more) {
    if (n >= CONFIG.flushMaxIterations) {
      logger.warn(
        `[Startup Sync] reached ${CONFIG.flushMaxIterations} iterations`,
      );
      break;
    }
    const tasks = workers.flatMap((w) => [
      w.hubToBranch.processBatch().catch((e) => {
        logger.error(`[Flush] ${w.branchId} h2b: ${e.message}`);
        return false;
      }),
      w.branchToHub.processBatch().catch((e) => {
        logger.error(`[Flush] ${w.branchId} b2h: ${e.message}`);
        return false;
      }),
    ]);
    const res = await Promise.all(tasks);
    more = res.some((h) => h === true);
    if (more) {
      n++;
      if (n % 50 === 0) logger.info(`[Startup Sync] ${n} batches`);
      await new Promise((r) => setImmediate(r));
    }
  }
  logger.info(`[Startup Sync] Complete. ${n} batches.`);
}

const app = express();
if (parseBoolEnv("TRUST_PROXY", false)) app.set("trust proxy", 1);
if (ALLOWED_ORIGINS.length) app.use(cors({ origin: ALLOWED_ORIGINS }));
app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  next();
});
app.use(
  rateLimit({
    windowMs: 60_000,
    max: parseIntEnv("RATE_LIMIT_MAX", 60, { min: 1 }),
    standardHeaders: true,
    legacyHeaders: false,
  }),
);
app.use(express.json({ limit: "100kb" }));
app.use((req, res, next) => {
  const s = Date.now();
  res.on("finish", () =>
    logger.debug(
      `[HTTP] ${req.method} ${req.path} ${res.statusCode} ${Date.now() - s}ms`,
    ),
  );
  next();
});
function requireApiKey(req, res, next) {
  const provided = req.get("x-api-key") || "";
  const hash = crypto.createHash("sha256").update(provided).digest();
  if (
    hash.length === API_SECRET_KEY_HASH.length &&
    crypto.timingSafeEqual(hash, API_SECRET_KEY_HASH)
  )
    return next();
  logger.warn(`[Auth] Rejected ${req.path} from ${req.ip}`);
  return res.status(401).json({ error: "Unauthorized" });
}

let syncManagers = [];
let managerById = new Map();
let rowCountCache = { ts: 0, hub: null, branches: new Map() };

async function approximateRowCount(node) {
  try {
    const r = await node.db.raw(
      `SELECT SUM(TABLE_ROWS) AS total FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN (?)`,
      [[...ALLOWED_TABLES]],
    );
    return Number((r[0] || r)[0]?.total || 0);
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
    branchNodes.map(async (b) =>
      branches.set(b.id, await approximateRowCount(b)),
    ),
  );
  rowCountCache = { ts: Date.now(), hub, branches };
  return rowCountCache;
}

app.get("/health", requireApiKey, async (req, res) => {
  let hubMax = null;
  try {
    const r = await HUB_NODE.db("sym_change_log").max("change_id as m").first();
    hubMax = r?.m != null ? BigInt(r.m) : 0n;
  } catch (err) {
    logger.error(`[Health] hub max: ${err.message}`);
  }
  const counts = await refreshRowCounts();
  const branchStatus = await Promise.all(
    branchNodes.map(async (b) => {
      const m = managerById.get(b.id);
      const h2b = m?.hubToBranch;
      const b2h = m?.branchToHub;
      try {
        const chk = await HUB_NODE.db("sym_checkpoint")
          .where("node_id", b.id)
          .first();
        const lastProc = chk?.last_processed_change_id
          ? BigInt(chk.last_processed_change_id)
          : 0n;
        const lag = hubMax != null ? (hubMax - lastProc).toString() : null;
        const halted = h2b?.halted || b2h?.halted;
        return {
          branchId: b.id,
          status: "CONNECTED",
          lastProcessedChangeId: lastProc.toString(),
          hubToBranchLag: lag,
          rowCounts: { hub: counts.hub, branch: counts.branches.get(b.id) },
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
          healthy: m ? m.consecutiveErrors === 0 && !halted : false,
          consecutiveErrors: m?.consecutiveErrors ?? null,
          lastError: m?.lastError ?? null,
          lastErrorAt: m?.lastErrorAt ?? null,
          lastRunAt: m?.lastRunAt ?? null,
          stats: m?.status() ?? null,
        };
      } catch (err) {
        return {
          branchId: b.id,
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
    relayEnabled: CONFIG.relayEnabled,
    branches: branchStatus,
  });
});

app.post("/resume/:branchId", requireApiKey, (req, res) => {
  const m = managerById.get(req.params.branchId);
  if (!m) return res.status(404).json({ error: "branch not found" });
  const h = m.hubToBranch.resume(),
    b = m.branchToHub.resume();
  logger.warn(`[Resume] ${req.params.branchId} h2b=${h} b2h=${b}`);
  res.json({
    branchId: req.params.branchId,
    resumed: { hubToBranch: h, branchToHub: b },
  });
});

let isShuttingDown = false;
let server;
let logFidelityInterval = null;

async function startMultiBranchEngine() {
  try {
    logger.info(`Starting Hub-and-Spoke for Hub [${HUB_NODE.id}]...`);
    logger.info(
      `[Config] port=${CONFIG.port} poll=${CONFIG.pollIntervalMs} batch=${CONFIG.batchSize} branches=${branchNodes.length} encryption=${Boolean(CONFIG.encryptionKey)} relay=${CONFIG.relayEnabled} verifyAfterApply=${CONFIG.verifyAfterApply}`,
    );

    await provisionNode(HUB_NODE);
    for (const branch of branchNodes) {
      try {
        await provisionNode(branch);
        logger.info(`[Startup] Provisioned ${branch.id}`);
      } catch (err) {
        logger.warn(`[Startup] ${branch.id} provision failed: ${err.message}`);
      }
    }

    const allForVerify = [HUB_NODE, ...branchNodes];
    const triggerProblems = await verifyAllTriggers(allForVerify);
    if (triggerProblems.length) {
      if (CONFIG.failOnMissingTriggers) {
        logger.error(
          `[Startup] Refusing to start: ${triggerProblems.length} node(s) with trigger problems. Set FAIL_ON_MISSING_TRIGGERS=false to override.`,
        );
        process.exit(1);
      } else
        logger.warn(
          `[Startup] ${triggerProblems.length} node(s) have trigger problems; continuing (FAIL_ON_MISSING_TRIGGERS=false)`,
        );
    }

    const flushList = [];
    for (const branch of branchNodes) {
      const h2b = new SyncWorker(HUB_NODE, branch);
      const b2h = new SyncWorker(branch, HUB_NODE);
      flushList.push({
        branchId: branch.id,
        hubToBranch: h2b,
        branchToHub: b2h,
      });
      const m = new BranchSyncManager(branch.id, h2b, b2h);
      syncManagers.push(m);
      managerById.set(branch.id, m);
      logger.info(`Registered sync worker for branch: ${branch.id}`);
    }

    await flushPendingSyncs(flushList);

    server = app.listen(CONFIG.port, () =>
      logger.info(`Hub Monitoring API on port ${CONFIG.port}`),
    );
    syncManagers.forEach((m) =>
      m
        .loop()
        .catch((err) =>
          logger.error(`[Fatal Loop] ${m.branchId}: ${err.stack}`),
        ),
    );
    logger.info(`All ${syncManagers.length} sync loops launched.`);

    // Start log-fidelity watchdog
    logFidelityInterval = setInterval(async () => {
      for (const n of [HUB_NODE, ...branchNodes]) {
        try {
          await logFidelityCheck(n);
        } catch (err) {
          logger.debug(`[LogFidelity] ${n.id}: ${err.message}`);
        }
      }
    }, CONFIG.logFidelityIntervalMs);
    logFidelityInterval.unref();
    logger.info(
      `[LogFidelity] Watchdog active every ${CONFIG.logFidelityIntervalMs}ms`,
    );
  } catch (err) {
    logger.error(`[Fatal Hub Engine]: ${err.stack}`);
    process.exit(1);
  }
}

async function shutdown(signal) {
  if (isShuttingDown) {
    logger.info("Forced shutdown.");
    process.exit(1);
  }
  isShuttingDown = true;
  logger.info(`Received ${signal}, shutting down...`);
  if (logFidelityInterval) clearInterval(logFidelityInterval);
  syncManagers.forEach((m) => m.stop());
  const t = setTimeout(() => {
    logger.error("Shutdown timeout.");
    process.exit(1);
  }, CONFIG.shutdownTimeoutMs);
  t.unref();
  if (server) {
    server.close();
    if (typeof server.closeIdleConnections === "function")
      server.closeIdleConnections();
  }
  try {
    await HUB_NODE.db.destroy();
  } catch (err) {
    logger.error(`Close hub: ${err.message}`);
  }
  const results = await Promise.allSettled(
    branchNodes.map((b) => b.db.destroy()),
  );
  results.forEach((r, i) => {
    if (r.status === "rejected")
      logger.error(`Close ${branchNodes[i].id}: ${r.reason?.message}`);
  });
  clearTimeout(t);
  logger.info("Shutdown complete.");
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("unhandledRejection", (r) =>
  logger.error(`[Unhandled Rejection]: ${r?.stack || r}`),
);
process.on("uncaughtException", (err) => {
  logger.error(`[Uncaught Exception]: ${err.stack || err.message}`);
  shutdown("uncaughtException");
});

startMultiBranchEngine();
