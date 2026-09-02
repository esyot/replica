require("dotenv").config();
const express = require("express");
const knex = require("knex");
const winston = require("winston");
const crypto = require("crypto");
const cors = require("cors");
const rateLimit = require("express-rate-limit");

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
    pool: { min: 0, max: 20, acquireTimeoutMillis: 5000 },
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
}

function filterPayloadColumns(tableName, payload) {
  const allowed = ALLOWED_COLUMNS[tableName];
  if (!allowed || !payload || typeof payload !== "object") return payload;
  const filtered = {};
  for (const key of allowed) {
    if (Object.prototype.hasOwnProperty.call(payload, key)) {
      filtered[key] = payload[key];
    }
  }
  return filtered;
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

    if (record) {
      return BigInt(record.last_processed_change_id);
    }

    const latestLog = await this.source
      .db("sym_change_log")
      .whereNot("node_source_id", this.target.id)
      .max("change_id as max_id")
      .first();

    const initialCheckpoint = latestLog?.max_id ? BigInt(latestLog.max_id) : 0n;
    await this.setCheckpoint(initialCheckpoint);

    logger.info(
      `[Checkpoint] Initialized ${this.source.id} -> ${this.target.id} at change_id: ${initialCheckpoint}`,
    );
    return initialCheckpoint;
  }

  async setCheckpoint(lastChangeId, trx) {
    const db = trx || this.target.db;
    const exists = await db("sym_checkpoint")
      .where("node_id", this.source.id)
      .first();
    if (exists) {
      await db("sym_checkpoint").where("node_id", this.source.id).update({
        last_processed_change_id: lastChangeId.toString(),
        updated_at: new Date(),
      });
    } else {
      await db("sym_checkpoint").insert({
        node_id: this.source.id,
        last_processed_change_id: lastChangeId.toString(),
        updated_at: new Date(),
      });
    }
  }

  async setSessionBypass(trx) {
    const client = this.target.client;
    if (client === "pg" || client === "postgres") {
      await trx.raw("SET LOCAL sym.is_syncing = 'true'");
    } else if (client === "mysql2" || client === "mysql") {
      await trx.raw("SET @sym_is_syncing = TRUE");
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

    return filterPayloadColumns(change.table_name, payload);
  }

  async applyTableChanges(trx, tableName, tableChanges) {
    const pkColumns = await getTablePkColumns(this.target, tableName);

    for (const change of tableChanges) {
      const payload = this.decodePayload(change);

      if (change.operation === "DELETE") {
        const deleteQuery = trx(tableName);
        if (pkColumns.length === 1) {
          deleteQuery.where(pkColumns[0], change.primary_key_val);
        } else {
          const pkVals = String(change.primary_key_val).split("-");
          pkColumns.forEach((col, idx) => deleteQuery.where(col, pkVals[idx]));
        }
        await deleteQuery.del();
      } else {
        const matchQuery = trx(tableName);
        if (pkColumns.length === 1) {
          const val =
            payload && payload[pkColumns[0]] !== undefined
              ? payload[pkColumns[0]]
              : change.primary_key_val;
          matchQuery.where(pkColumns[0], val);
        } else {
          const pkVals = String(change.primary_key_val).split("-");
          pkColumns.forEach((col, idx) => {
            const val =
              payload && payload[col] !== undefined
                ? payload[col]
                : pkVals[idx];
            matchQuery.where(col, val);
          });
        }

        const existingRecord = await matchQuery.first();

        if (!existingRecord) {
          await trx(tableName).insert(payload);
        } else {
          const updateQuery = trx(tableName);
          if (pkColumns.length === 1) {
            updateQuery.where(pkColumns[0], existingRecord[pkColumns[0]]);
          } else {
            pkColumns.forEach((col) =>
              updateQuery.where(col, existingRecord[col]),
            );
          }
          await updateQuery.update(payload);
        }
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

      if (changes.length === 0) {
        return false;
      }

      logger.info(
        `[Sync Engine] ${this.source.id} -> ${this.target.id} | Processing ${changes.length} events...`,
      );

      const byTable = new Map();
      for (const change of changes) {
        if (!ALLOWED_TABLES.has(change.table_name)) continue;
        if (!byTable.has(change.table_name)) byTable.set(change.table_name, []);
        byTable.get(change.table_name).push(change);
      }

      await this.target.db.transaction(async (trx) => {
        await this.setSessionBypass(trx);

        for (const [tableName, tableChanges] of byTable) {
          await this.applyTableChanges(trx, tableName, tableChanges);
        }

        const lastBatchId = changes[changes.length - 1].change_id;
        await this.setCheckpoint(lastBatchId, trx);
      });

      return true;
    } finally {
      this.isProcessing = false;
    }
  }
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

app.get("/health", requireApiKey, async (req, res) => {
  const branchStatus = [];
  for (const branch of branchNodes) {
    try {
      const lastCheck = await HUB_NODE.db("sym_checkpoint")
        .where("node_id", branch.id)
        .first();
      branchStatus.push({
        branchId: branch.id,
        status: "CONNECTED",
        lastProcessedChangeId: lastCheck?.last_processed_change_id
          ? lastCheck.last_processed_change_id.toString()
          : "0",
      });
    } catch (err) {
      branchStatus.push({
        branchId: branch.id,
        status: "OFFLINE/UNREACHABLE",
        error: err.message,
      });
    }
  }

  res.json({
    hub: HUB_NODE.id,
    activeBranches: branchStatus.length,
    branches: branchStatus,
  });
});

let running = true;
let isShuttingDown = false;
let server;

async function startMultiBranchEngine() {
  try {
    logger.info(`Starting Hub-and-Spoke Engine for Hub [${HUB_NODE.id}]...`);

    await provisionNode(HUB_NODE);

    const workers = [];

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

      workers.push({
        branchId: branch.id,
        hubToBranch: new SyncWorker(HUB_NODE, branch),
        branchToHub: new SyncWorker(branch, HUB_NODE),
      });

      logger.info(`Registered sync worker for branch: [${branch.id}]`);
    }

    server = app.listen(CONFIG.port, () => {
      logger.info(`Hub Monitoring API active on port ${CONFIG.port}`);
    });

    while (running) {
      const tasks = workers.flatMap((w) => [
        w.hubToBranch.processBatch().catch((err) => {
          logger.error(
            `[Sync Error] ${w.branchId} hub->branch: ${err.stack || err.message}`,
          );
          return false;
        }),
        w.branchToHub.processBatch().catch((err) => {
          logger.error(
            `[Sync Error] ${w.branchId} branch->hub: ${err.stack || err.message}`,
          );
          return false;
        }),
      ]);

      await Promise.all(tasks);
      if (!running) break;
      await new Promise((resolve) =>
        setTimeout(resolve, CONFIG.pollIntervalMs),
      );
    }

    logger.info("Sync loop stopped, closing connections...");
    await HUB_NODE.db.destroy();
    await Promise.all(branchNodes.map((b) => b.db.destroy()));
    logger.info("Shutdown complete.");
    process.exit(0);
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
  running = false;

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

  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

startMultiBranchEngine();
