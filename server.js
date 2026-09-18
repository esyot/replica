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

const API_SECRET_KEY = process.env.API_SECRET_KEY;
if (!API_SECRET_KEY) {
  logger.error("CRITICAL: API_SECRET_KEY missing!");
  process.exit(1);
}

const API_SECRET_KEY_BUF = Buffer.from(API_SECRET_KEY);

let ALLOWED_COLUMNS = {};
try {
  ALLOWED_COLUMNS = JSON.parse(process.env.ALLOWED_COLUMNS_JSON || "{}");
} catch (err) {
  logger.error(`Invalid ALLOWED_COLUMNS_JSON, ignoring: ${err.message}`);
}

const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((o) => o.trim())
  .filter(Boolean);

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
    pool: {
      min: 4,
      max: parseInt(process.env.HUB_POOL_MAX || "50", 10),
      acquireTimeoutMillis: 5000,
      idleTimeoutMillis: 30000,
    },
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
      pool: {
        min: 0,
        max: 10,
        idleTimeoutMillis: 30000,
        acquireTimeoutMillis: 5000,
      },
    }),
  };
});

const CONFIG = {
  port: parseInt(process.env.PORT || "3000", 10),
  pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS || "2000", 10),
  batchSize: parseInt(process.env.BATCH_SIZE || "500", 10),
  encryptionKey: process.env.PAYLOAD_ENCRYPTION_KEY || null,
  setupTriggersOnStartup: process.env.SETUP_TRIGGERS_ON_STARTUP === "true",
  maxBackoffMs: parseInt(process.env.MAX_BACKOFF_MS || "30000", 10),
};

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

async function provisionNode(node) {
  const db = node.db;
  const client = node.client;

  if (client === "mysql2" || client === "mysql") {
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
  }

  const hasCheckpoint = await db.schema.hasTable("sym_checkpoint");
  if (!hasCheckpoint) {
    await db.schema.createTable("sym_checkpoint", (table) => {
      table.string("node_id", 50).primary();
      table.bigInteger("last_processed_change_id").notNullable();
      table.timestamp("updated_at").defaultTo(db.fn.now());
    });
  }

  if (client === "pg" || client === "postgres") {
    await db.raw(
      "CREATE INDEX IF NOT EXISTS idx_sym_log_sync_poll ON sym_change_log (change_id ASC, node_source_id);",
    );
    await db.raw(
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_sym_checkpoint_node ON sym_checkpoint (node_id);",
    );
  } else if (client === "mysql2" || client === "mysql") {
    try {
      await db.raw(
        "CREATE INDEX idx_sym_log_sync_poll ON sym_change_log (change_id ASC, node_source_id);",
      );
    } catch (err) {
      if (!err.message.includes("Duplicate key name")) throw err;
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

const PK_CACHE = new Map();

async function getCachedPkColumns(node, tableName) {
  const cacheKey = `${node.id}:${tableName}`;
  if (!PK_CACHE.has(cacheKey)) {
    const cols = await getTablePkColumns(node, tableName);
    PK_CACHE.set(cacheKey, cols);
  }
  return PK_CACHE.get(cacheKey);
}

class SyncWorker {
  constructor(source, target) {
    this.source = source;
    this.target = target;
    this.isProcessing = false;
  }

  async getCheckpoint() {
    const record = await this.target
      .db("sym_checkpoint")
      .where("node_id", this.source.id)
      .first();

    let currentCheckpoint = record
      ? BigInt(record.last_processed_change_id)
      : 0n;

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

  async setSessionBypass(trx) {
    const client = this.target.client;
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

    try {
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
              pkColumns.forEach((col, idx) =>
                deleteQuery.where(col, pkVals[idx]),
              );
              return deleteQuery.del();
            }),
          );
        }
      }

      if (upserts.length > 0) {
        await trx(tableName).insert(upserts).onConflict(pkColumns).merge();
      }
    } catch (err) {
      logger.error(
        `[DB Sync Error] Table: ${tableName} | Code: ${err.code || "N/A"} | Details: ${err.sqlMessage || err.message}`,
      );
      throw err;
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

      if (changes.length === 0) {
        return false;
      }

      const byTable = new Map();
      let validEventCount = 0;

      for (const change of changes) {
        if (!ALLOWED_TABLES.has(change.table_name)) continue;
        if (!byTable.has(change.table_name)) byTable.set(change.table_name, []);
        byTable.get(change.table_name).push(change);
        validEventCount++;
      }

      const lastBatchId = changes[changes.length - 1].change_id;

      if (validEventCount === 0) {
        await this.setCheckpoint(lastBatchId);
        return changes.length === CONFIG.batchSize;
      }

      logger.info(
        `[Sync Engine] ${this.source.id} -> ${this.target.id} | Processing ${validEventCount} events...`,
      );

      await this.target.db.transaction(async (trx) => {
        await this.setSessionBypass(trx);

        for (const [tableName, tableChanges] of byTable) {
          await this.applyTableChanges(trx, tableName, tableChanges);
        }

        await this.setCheckpoint(lastBatchId, trx);

        if (this.target.client === "mysql2" || this.target.client === "mysql") {
          await trx.raw("SET FOREIGN_KEY_CHECKS = 1;");
        }
      });

      return changes.length === CONFIG.batchSize;
    } finally {
      this.isProcessing = false;
    }
  }
}

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
        this.consecutiveErrors = 0;
        this.lastError = null;
      } catch (err) {
        hadError = true;
        this.consecutiveErrors++;
        this.lastError = err.message;
        logger.error(
          `[${this.branchId}] sync error (#${this.consecutiveErrors}): ${err.stack || err.message}`,
        );
      }

      this.lastRunAt = new Date();

      if (!this.running) break;

      if (hadWork && !hadError) continue;

      const delay = hadError
        ? Math.min(
            this.pollIntervalMs * 2 ** Math.min(this.consecutiveErrors, 6),
            this.maxBackoffMs,
          )
        : this.pollIntervalMs;

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
    };
  }
}

async function flushPendingSyncs(workers) {
  logger.info(
    "[Startup Sync] Checking and draining pending backlog across all nodes...",
  );
  let pendingCount = 0;
  let hasMoreData = true;

  while (hasMoreData) {
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
      pendingCount++;
    }
  }

  logger.info(
    `[Startup Sync] Flush complete. Flushed ${pendingCount} batches of pending changes. All nodes caught up!`,
  );
}

const app = express();

if (ALLOWED_ORIGINS.length > 0) {
  app.use(cors({ origin: ALLOWED_ORIGINS }));
}

app.use(
  rateLimit({
    windowMs: 60 * 1000,
    max: 60,
    standardHeaders: true,
    legacyHeaders: false,
  }),
);

app.use(express.json());

function requireApiKey(req, res, next) {
  const provided = req.get("x-api-key") || "";
  const providedBuf = Buffer.from(provided);
  const isValid =
    providedBuf.length === API_SECRET_KEY_BUF.length &&
    crypto.timingSafeEqual(providedBuf, API_SECRET_KEY_BUF);
  if (!isValid) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

let syncManagers = [];

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
      const manager = syncManagers.find((m) => m.branchId === branch.id);
      try {
        const lastCheck = await HUB_NODE.db("sym_checkpoint")
          .where("node_id", branch.id)
          .first();

        const lastProcessed = lastCheck?.last_processed_change_id
          ? BigInt(lastCheck.last_processed_change_id)
          : 0n;

        return {
          branchId: branch.id,
          status: "CONNECTED",
          lastProcessedChangeId: lastProcessed.toString(),
          hubToBranchLag:
            hubMaxChangeId != null
              ? (hubMaxChangeId - lastProcessed).toString()
              : null,
          consecutiveErrors: manager?.consecutiveErrors ?? null,
          lastError: manager?.lastError ?? null,
          lastRunAt: manager?.lastRunAt ?? null,
        };
      } catch (err) {
        return {
          branchId: branch.id,
          status: "OFFLINE/UNREACHABLE",
          error: err.message,
          consecutiveErrors: manager?.consecutiveErrors ?? null,
        };
      }
    }),
  );

  res.json({
    hub: HUB_NODE.id,
    activeBranches: branchStatus.length,
    branches: branchStatus,
  });
});

let isShuttingDown = false;
let server;

async function startMultiBranchEngine() {
  try {
    logger.info(`Starting Hub-and-Spoke Engine for Hub [${HUB_NODE.id}]...`);

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

  setTimeout(() => {
    logger.error("Forcefully shutting down due to timeout.");
    process.exit(1);
  }, 2000).unref();

  if (server) {
    server.close();
  }

  try {
    await HUB_NODE.db.destroy();
    await Promise.all(branchNodes.map((b) => b.db.destroy()));
  } catch (err) {
    logger.error(`Error closing DB connections: ${err.message}`);
  }

  logger.info("Shutdown complete.");
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

startMultiBranchEngine();
