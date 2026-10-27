#!/usr/bin/env node
/**
 * copy-db.js — the single ops tool. Four modes:
 *
 *   --reseed      Copy one node's entire database into one or all other
 *                 nodes. Backs up target(s), drops triggers, truncates,
 *                 copies via mysqldump | mysql, rebuilds triggers, resets
 *                 sym_change_log and sym_checkpoint.
 *
 *   --verify      Read-only. Compares PK sets and (optionally) row contents
 *                 across hub and branches. Uses merge-join for O(1) memory.
 *
 *   --fix-logs    Read-only analysis first; with --apply, relabels mislabeled
 *                 sym_change_log entries per node.
 *
 *   --ping        Reachability check (SELECT 1 with timeout) for every node.
 *
 * Common flags:
 *   --from=ID         Source node (hub or branch id) for --reseed
 *   --to=ID|all       Target node(s). "all" = every node except source.
 *   --apply           Actually write. Without it, everything is dry-run.
 *   --tables=t1,t2    Restrict to specific tables
 *   --skip-triggers   Skip drop/rebuild triggers on target (reseed only)
 *   --keep-log        Don't truncate sym_change_log after reseed
 *   --keep-checkpoint Don't truncate sym_checkpoint after reseed
 *   --no-backup       Skip backup phase (faster, riskier)
 *   --full / --no-full For --verify: compare row contents (default: on)
 *   --max-report=N    For --verify: cap sample report (default 50)
 *   --skip-preflight  Skip pre-flight checks (dangerous, for retries)
 *
 * Node resolution: reads LOCAL_* for hub, BRANCHES_JSON for branches.
 * If --from is omitted with --to=all, prompts interactively for source.
 *
 * Env vars (optional):
 *   CHECK_TIMEOUT_MS       Ping timeout per node (default 3000)
 *   RESEED_TIMEOUT_MS      Per-phase timeout for dump/load (default 300000)
 *   BACKUP_DIR             Where to write backups (default ./backups/)
 *   BACKUP_RETENTION_DAYS  Auto-delete backups older than N days (default 7)
 *
 * Requires: mysqldump and mysql on PATH. MySQL/MariaDB only.
 *
 * Exit codes:
 *   0 = success / all in parity / all mislabels fixed / all nodes online
 *   1 = partial failure / gaps remain / some nodes offline
 *   2 = fatal error / refused
 */

require("dotenv").config();
const knex = require("knex");
const winston = require("winston");
const { spawn, execSync } = require("child_process");
const { URL } = require("url");
const fs = require("fs");
const path = require("path");
const readline = require("readline");

const {
  setupNodeTriggers,
  getTablePkColumns,
  ALLOWED_TABLES,
} = require("./triggers");

/* ------------------------------------------------------------------ */
/* Logger                                                             */
/* ------------------------------------------------------------------ */

const logger = winston.createLogger({
  level: "info",
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
/* Env config                                                        */
/* ------------------------------------------------------------------ */

const CHECK_TIMEOUT_MS = parseInt(process.env.CHECK_TIMEOUT_MS || "3000", 10);
const RESEED_TIMEOUT_MS = parseInt(
  process.env.RESEED_TIMEOUT_MS || "300000",
  10,
);
const BACKUP_DIR = process.env.BACKUP_DIR || "./backups";
const BACKUP_RETENTION_DAYS = parseInt(
  process.env.BACKUP_RETENTION_DAYS || "7",
  10,
);

/* ------------------------------------------------------------------ */
/* CLI                                                                */
/* ------------------------------------------------------------------ */

const argv = process.argv.slice(2);
const opts = {
  mode: null,
  from: null,
  to: null,
  apply: false,
  skipTriggers: false,
  keepLog: false,
  keepCheckpoint: false,
  backup: true,
  tables: null,
  full: true,
  maxReport: 50,
  skipPreflight: false,
};

for (const a of argv) {
  if (a === "--apply") opts.apply = true;
  else if (a === "--dry-run") opts.apply = false;
  else if (a === "--skip-triggers") opts.skipTriggers = true;
  else if (a === "--keep-log") opts.keepLog = true;
  else if (a === "--keep-checkpoint") opts.keepCheckpoint = true;
  else if (a === "--no-backup") opts.backup = false;
  else if (a === "--skip-preflight") opts.skipPreflight = true;
  else if (a === "--reseed") opts.mode = "reseed";
  else if (a === "--verify") opts.mode = "verify";
  else if (a === "--fix-logs") opts.mode = "fix-logs";
  else if (a === "--ping") opts.mode = "ping";
  else if (a === "--full") opts.full = true;
  else if (a === "--no-full") opts.full = false;
  else if (a.startsWith("--from=")) opts.from = a.slice(7);
  else if (a.startsWith("--to=")) opts.to = a.slice(5);
  else if (a.startsWith("--tables="))
    opts.tables = a
      .slice(9)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  else if (a.startsWith("--max-report="))
    opts.maxReport = parseInt(a.slice(13), 10);
  else {
    logger.error(`Unknown arg: ${a}`);
    process.exit(2);
  }
}

if (!opts.mode) {
  if (opts.to || opts.from) opts.mode = "reseed";
  else {
    logger.error(
      "One of --reseed, --verify, --fix-logs, --ping is required (or pass --from/--to for reseed)",
    );
    process.exit(2);
  }
}

if (opts.mode === "reseed" && !opts.to) {
  logger.error("--to is required for --reseed (a node ID, or 'all')");
  process.exit(2);
}

if (!opts.apply && opts.mode !== "ping" && opts.mode !== "verify") {
  logger.info(
    "==================================================================\n" +
      "  DRY RUN MODE — no writes will happen.\n" +
      "  Re-run with --apply to execute.\n" +
      "==================================================================",
  );
} else if (opts.apply && opts.mode === "reseed") {
  logger.warn(
    "==================================================================\n" +
      "  APPLY MODE — this will WIPE the target node(s)' data tables.\n" +
      "  Ctrl+C within 10 seconds to abort.\n" +
      "==================================================================",
  );
}

/* ------------------------------------------------------------------ */
/* Signal handling — kill child processes on Ctrl+C                  */
/* ------------------------------------------------------------------ */

const runningChildren = new Set();

function spawnTracked(cmd, args, spawnOpts) {
  const child = spawn(cmd, args, spawnOpts);
  runningChildren.add(child);
  child.on("close", () => runningChildren.delete(child));
  child.on("error", () => runningChildren.delete(child));
  return child;
}

function killChildren() {
  for (const c of runningChildren) {
    try {
      c.kill("SIGTERM");
    } catch {
      /* ignore */
    }
  }
}

process.on("SIGINT", () => {
  logger.warn("SIGINT — killing child processes...");
  killChildren();
  process.exit(130);
});
process.on("SIGTERM", () => {
  logger.warn("SIGTERM — killing child processes...");
  killChildren();
  process.exit(143);
});

/* ------------------------------------------------------------------ */
/* Node resolution                                                    */
/* ------------------------------------------------------------------ */

function normalizeClient(c) {
  if (!c) return "mysql2";
  c = c.toLowerCase().trim();
  if (c === "mysql") return "mysql2";
  if (c === "postgresql") return "pg";
  return c;
}
function isPg(c) {
  return c === "pg" || c === "postgres";
}

function resolveNodes() {
  const all = [];
  if (process.env.LOCAL_URL) {
    all.push({
      id: process.env.LOCAL_ID || "NODE_MAIN_HUB",
      client: normalizeClient(process.env.LOCAL_CLIENT),
      url: process.env.LOCAL_URL,
      isHub: true,
    });
  }
  for (const b of JSON.parse(process.env.BRANCHES_JSON || "[]")) {
    all.push({
      id: b.id,
      client: normalizeClient(b.client),
      url: b.url,
      isHub: false,
    });
  }
  return all;
}

function parseMysqlUrl(urlStr) {
  const u = new URL(urlStr);
  return {
    host: u.hostname,
    port: u.port ? parseInt(u.port, 10) : 3306,
    user: decodeURIComponent(u.username || ""),
    password: decodeURIComponent(u.password || ""),
    database: u.pathname.slice(1),
  };
}

function buildKnexFor(conn) {
  return knex({
    client: "mysql2",
    connection: {
      host: conn.host,
      port: conn.port,
      user: conn.user,
      password: conn.password,
      database: conn.database,
      multipleStatements: true,
    },
    pool: { min: 0, max: 4 },
  });
}

/* ------------------------------------------------------------------ */
/* MySQL client options file (passwords not on command line)         */
/* ------------------------------------------------------------------ */

const tempFiles = new Set();

function makeDefaultsFile(conn) {
  const dir = path.join(require("os").tmpdir());
  const tmp = path.join(
    dir,
    `.my.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.cnf`,
  );
  const content = `[client]
host=${conn.host}
port=${conn.port}
user=${conn.user}
password=${conn.password}
`;
  fs.writeFileSync(tmp, content, { mode: 0o600 });
  tempFiles.add(tmp);
  return tmp;
}

function cleanupTempFiles() {
  for (const f of tempFiles) {
    try {
      fs.unlinkSync(f);
    } catch {
      /* ignore */
    }
  }
  tempFiles.clear();
}

process.on("exit", cleanupTempFiles);

/* ------------------------------------------------------------------ */
/* Interactive prompt                                                 */
/* ------------------------------------------------------------------ */

function prompt(question) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

async function pickSourceInteractively(nodes) {
  logger.info("\nAvailable nodes:");
  nodes.forEach((n, i) => {
    logger.info(`  [${i + 1}] ${n.id}${n.isHub ? " (hub)" : " (branch)"}`);
    logger.info(`      ${n.url.replace(/\/\/[^@]+@/, "//***@")}`);
  });
  logger.info("");
  while (true) {
    const answer = await prompt(
      `Which node should be the SOURCE (1-${nodes.length})? `,
    );
    const idx = parseInt(answer, 10);
    if (!Number.isNaN(idx) && idx >= 1 && idx <= nodes.length) {
      return nodes[idx - 1];
    }
    logger.warn(
      `Invalid choice "${answer}". Enter a number 1-${nodes.length}.`,
    );
  }
}

async function confirmApply(targets) {
  logger.info("");
  logger.info("About to WIPE data tables on:");
  targets.forEach((t) => logger.info(`  - ${t.id}`));
  logger.info("");
  logger.info("Are you SURE? This is destructive and irreversible without");
  logger.info(`the backup file in ${BACKUP_DIR}/.`);
  const answer = await prompt(
    `Type "yes" to proceed, anything else to abort: `,
  );
  return answer.toLowerCase() === "yes";
}

/* ------------------------------------------------------------------ */
/* FK-safe table order                                                */
/* ------------------------------------------------------------------ */

const TABLE_ORDER = [
  "users",
  "roles",
  "permissions",
  "store",
  "store_address",
  "store_user",
  "customer",
  "customer_address",
  "employees",
  "supplier",
  "system",
  "system_user",
  "classification",
  "unit_of_measure",
  "product_type",
  "product_category",
  "product",
  "product_image",
  "pricing_scheme",
  "price",
  "discount",
  "discount_customer",
  "stocks_on_hand",
  "damaged_stocks_on_hand",
  "stock_costs",
  "stock_cost_details",
  "stock_cost_movement_log",
  "in_out_stocks",
  "in_out_stocks_details",
  "in_out_damaged_stocks",
  "in_out_damaged_stocks_details",
  "stock_requests",
  "stock_request_items",
  "purchase_order",
  "purchase_order_details",
  "purchase_order_receive_details",
  "pos_session",
  "cash_count",
  "sales_order",
  "sales_order_details",
  "sales_order_queue",
  "sales_payment",
  "card",
  "card_transaction",
  "return_item",
  "adjustment",
  "deposit",
  "layaway",
  "layaway_item",
  "store_expense",
  "store_expenses_item",
  "repa_group",
  "repa_group_schedule",
  "repa_member",
  "repa_member_winner",
  "repa_deposit",
  "repa_gratis_log",
  "system_log",
  "address",
  "receipt_layouts",
  "receipt_placeholders",
  "model_has_roles",
  "model_has_permissions",
  "role_has_permissions",
  "user_role",
];

function sortByDependency(tables) {
  const idx = new Map(TABLE_ORDER.map((t, i) => [t, i]));
  return [...tables].sort((a, b) => {
    const ai = idx.has(a) ? idx.get(a) : 9999;
    const bi = idx.has(b) ? idx.get(b) : 9999;
    return ai !== bi ? ai - bi : a.localeCompare(b);
  });
}

/* ------------------------------------------------------------------ */
/* PK helpers                                                         */
/* ------------------------------------------------------------------ */

function pkKeyFromRow(row, cols) {
  if (cols.length === 1) {
    const v = row[cols[0]];
    return v == null ? null : String(v);
  }
  return JSON.stringify(cols.map((c) => row[c] ?? null));
}

function resolveTables() {
  const tables = opts.tables
    ? opts.tables.filter((t) => ALLOWED_TABLES.has(t))
    : [...ALLOWED_TABLES];
  if (tables.length === 0) {
    logger.error("No tables to process (check --tables / ALLOWED_TABLES)");
    process.exit(2);
  }
  return sortByDependency(tables);
}

/* ------------------------------------------------------------------ */
/* Utilities                                                          */
/* ------------------------------------------------------------------ */

async function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} timeout after ${ms}ms`)),
      ms,
    );
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function ensureBackupDir() {
  if (!fs.existsSync(BACKUP_DIR)) {
    fs.mkdirSync(BACKUP_DIR, { recursive: true });
  }
}

function uniqueBackupPath(targetId) {
  ensureBackupDir();
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  let file = path.join(BACKUP_DIR, `backup-${targetId}-${ts}.sql`);
  let n = 2;
  while (fs.existsSync(file)) {
    file = path.join(BACKUP_DIR, `backup-${targetId}-${ts}-${n}.sql`);
    n++;
  }
  return file;
}

function cleanupOldBackups() {
  if (!fs.existsSync(BACKUP_DIR)) return;
  const cutoff = Date.now() - BACKUP_RETENTION_DAYS * 24 * 60 * 60 * 1000;
  let removed = 0;
  for (const f of fs.readdirSync(BACKUP_DIR)) {
    if (!f.startsWith("backup-") || !f.endsWith(".sql")) continue;
    const full = path.join(BACKUP_DIR, f);
    try {
      const stat = fs.statSync(full);
      if (stat.mtimeMs < cutoff) {
        fs.unlinkSync(full);
        removed++;
      }
    } catch {
      /* ignore */
    }
  }
  if (removed > 0) {
    logger.info(
      `[backups] Removed ${removed} backup(s) older than ${BACKUP_RETENTION_DAYS} days`,
    );
  }
}

/* ------------------------------------------------------------------ */
/* Preflight                                                          */
/* ------------------------------------------------------------------ */

async function preflight(source, targets, tables) {
  logger.info("Running pre-flight checks...");

  // 1. Binaries
  try {
    execSync("which mysqldump", { stdio: "ignore" });
    execSync("which mysql", { stdio: "ignore" });
  } catch {
    throw new Error("mysqldump or mysql not found on PATH");
  }

  // 2. Sync must be stopped — no recent writes to sym_change_log on any node
  const allNodes = [source, ...targets];
  for (const n of allNodes) {
    const db = buildKnexFor(parseMysqlUrl(n.url));
    try {
      const recent = await db("sym_change_log")
        .where("created_at", ">", new Date(Date.now() - 60000))
        .count("* as c")
        .first();
      const cnt = Number(recent?.c || 0);
      if (cnt > 0) {
        throw new Error(
          `[${n.id}] has ${cnt} log entries in the last 60s — sync appears to be RUNNING. Stop server.js first (or use --skip-preflight).`,
        );
      }
    } catch (err) {
      if (/doesn't exist|no such table/i.test(err.message)) continue;
      if (/sync appears to be RUNNING/.test(err.message)) throw err;
    } finally {
      await db.destroy().catch(() => {});
    }
  }

  // 3. Source has all tables
  const srcDb = buildKnexFor(parseMysqlUrl(source.url));
  const srcTables = await srcDb.raw(
    `SELECT TABLE_NAME FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'`,
  );
  const srcSet = new Set((srcTables[0] || srcTables).map((r) => r.TABLE_NAME));
  const missingOnSource = tables.filter((t) => !srcSet.has(t));
  if (missingOnSource.length > 0) {
    throw new Error(
      `Source [${source.id}] missing tables: ${missingOnSource.join(", ")}`,
    );
  }

  // 4. Source has non-zero total rows (catches wrong-source mistake)
  let totalRows = 0;
  for (const t of tables) {
    try {
      const c = await srcDb(t).count("* as c").first();
      totalRows += Number(c?.c || 0);
    } catch {
      /* ignore */
    }
  }
  if (totalRows === 0) {
    throw new Error(
      `Source [${source.id}] has 0 rows across all tables — refusing to reseed`,
    );
  }
  await srcDb.destroy();

  // 5. Target has all tables (schema must match)
  for (const tgt of targets) {
    const tgtDb = buildKnexFor(parseMysqlUrl(tgt.url));
    const tgtTables = await tgtDb.raw(
      `SELECT TABLE_NAME FROM information_schema.TABLES
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'`,
    );
    const tgtSet = new Set(
      (tgtTables[0] || tgtTables).map((r) => r.TABLE_NAME),
    );
    const missingOnTgt = tables.filter((t) => !tgtSet.has(t));
    if (missingOnTgt.length > 0) {
      throw new Error(
        `Target [${tgt.id}] missing tables: ${missingOnTgt.join(", ")}`,
      );
    }
    await tgtDb.destroy();
  }

  logger.info(
    `Pre-flight OK (source ${totalRows} total rows, ${tables.length} tables, ${targets.length} targets)`,
  );
}

/* ------------------------------------------------------------------ */
/* MODE: ping                                                         */
/* ------------------------------------------------------------------ */

async function ping(nodes, timeoutMs) {
  logger.info(`Pinging ${nodes.length} node(s) (timeout ${timeoutMs}ms)...\n`);

  const results = await Promise.all(
    nodes.map(async (n) => {
      const db = buildKnexFor(parseMysqlUrl(n.url));
      const start = Date.now();
      try {
        await withTimeout(db.raw("SELECT 1"), timeoutMs, "ping");
        return {
          node: n.id,
          type: n.isHub ? "hub" : "branch",
          status: "ONLINE",
          latency: Date.now() - start,
          detail: "",
        };
      } catch (err) {
        const reason = /timeout/i.test(err.message)
          ? `timed out after ${timeoutMs}ms`
          : err.code || err.message;
        return {
          node: n.id,
          type: n.isHub ? "hub" : "branch",
          status: "OFFLINE",
          latency: null,
          detail: String(reason).slice(0, 80),
        };
      } finally {
        await db.destroy().catch(() => {});
      }
    }),
  );

  console.table(
    results.map((r) => ({
      node: r.node,
      type: r.type,
      status: r.status === "ONLINE" ? "🟢 ONLINE" : "🔴 OFFLINE",
      latency: r.latency !== null ? `${r.latency} ms` : "-",
      detail: r.detail,
    })),
  );

  const online = results.filter((r) => r.status === "ONLINE").length;
  const offline = results.length - online;
  logger.info(
    `Summary: ${online}/${results.length} online, ${offline} offline`,
  );
  return offline > 0 ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* MODE: reseed                                                       */
/* ------------------------------------------------------------------ */

async function reseed(source, targets, tables) {
  // Run targets in parallel — both read from the same source snapshot
  const results = await Promise.all(
    targets.map(async (tgt) => {
      logger.info(`\n========== Reseed ${source.id} -> ${tgt.id} ==========`);
      try {
        const ok = await reseedOne(source, tgt, tables);
        return { target: tgt.id, ok };
      } catch (err) {
        logger.error(`[${tgt.id}] threw: ${err.message}`);
        return { target: tgt.id, ok: false, error: err.message };
      }
    }),
  );

  let allOk = true;
  for (const r of results) {
    if (!r.ok) {
      allOk = false;
      logger.error(`[${r.target}] FAILED: ${r.error || "see logs above"}`);
    }
  }
  return allOk;
}

async function reseedOne(source, target, tables) {
  const srcConn = parseMysqlUrl(source.url);
  const tgtConn = parseMysqlUrl(target.url);
  const backupFile = uniqueBackupPath(target.id);

  if (target.id === process.env.LOCAL_ID) {
    logger.warn(
      `Target [${target.id}] is THIS machine — ensure server.js is stopped here too`,
    );
  }

  // Phase 1: backup
  if (opts.backup) {
    logger.info(`[1/7] Backing up [${target.id}] -> ${backupFile}`);
    try {
      await withTimeout(
        runDump(tgtConn, tables, backupFile),
        RESEED_TIMEOUT_MS,
        "backup",
      );
      const size = (fs.statSync(backupFile).size / 1024 / 1024).toFixed(1);
      logger.info(`[1/7] Backup written (${size} MB)`);
    } catch (err) {
      logger.error(`Backup failed: ${err.message}`);
      return false;
    }
  } else logger.warn("[1/7] Skipping backup (--no-backup)");

  // Phase 2: drop triggers
  if (!opts.skipTriggers) {
    logger.info(`[2/7] Dropping triggers on [${target.id}]`);
    try {
      const db = buildKnexFor(tgtConn);
      const r = await db.raw(
        `SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
         WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME LIKE 'sym_trig_%'`,
      );
      const list = r[0] || r;
      for (const t of list) {
        await db.raw(`DROP TRIGGER IF EXISTS \`${t.TRIGGER_NAME}\``);
      }
      logger.info(`[2/7] Dropped ${list.length} trigger(s)`);
      await db.destroy();
    } catch (err) {
      logger.error(`Trigger drop failed: ${err.message}`);
      return false;
    }
  } else logger.warn("[2/7] Skipping trigger drop (--skip-triggers)");

  // Phase 3: truncate
  logger.info(`[3/7] Truncating ${tables.length} tables on [${target.id}]`);
  try {
    const db = buildKnexFor(tgtConn);
    await db.raw("SET FOREIGN_KEY_CHECKS = 0");
    let truncated = 0;
    for (const tbl of tables) {
      try {
        await db.raw(`TRUNCATE TABLE \`${tbl}\``);
        truncated++;
      } catch (err) {
        logger.warn(`  truncate ${tbl}: ${err.message}`);
      }
    }
    await db.raw("SET FOREIGN_KEY_CHECKS = 1");
    await db.destroy();
    logger.info(`[3/7] Truncated ${truncated}/${tables.length} tables`);
  } catch (err) {
    logger.error(`Truncate failed: ${err.message}`);
    return false;
  }

  // Phase 4: clear change logs BEFORE data copy (so no new entries land in the window)
  if (!opts.keepLog) {
    logger.info(
      `[4/7] TRUNCATE sym_change_log on [${source.id}] and [${target.id}]`,
    );
    for (const n of [source, target]) {
      try {
        const db = buildKnexFor(parseMysqlUrl(n.url));
        await db.raw("TRUNCATE TABLE sym_change_log");
        logger.info(`  [${n.id}] sym_change_log cleared`);
        await db.destroy();
      } catch (err) {
        if (!/doesn't exist/i.test(err.message)) {
          logger.warn(`  [${n.id}]: ${err.message}`);
        }
      }
    }
  } else logger.warn("[4/7] Keeping sym_change_log (--keep-log)");

  // Phase 5: pipe dump
  logger.info(`[5/7] Copying data: ${source.id} -> ${target.id}`);
  try {
    await withTimeout(
      pipeDump(srcConn, tgtConn, tables),
      RESEED_TIMEOUT_MS,
      "data copy",
    );
    logger.info(`[5/7] Data copy complete`);
  } catch (err) {
    logger.error(`Data copy failed: ${err.message}`);
    logger.error(
      `Target may be inconsistent. Restore from ${backupFile} if needed.`,
    );
    return false;
  }

  // Phase 6: rebuild triggers
  if (!opts.skipTriggers) {
    logger.info(`[6/7] Rebuilding triggers on [${target.id}]`);
    try {
      const db = buildKnexFor(tgtConn);
      const summary = await setupNodeTriggers({
        id: target.id,
        client: "mysql2",
        db,
      });
      logger.info(
        `[6/7] Rebuilt: ok=${summary.ok.length} skipped=${summary.skipped.length} failed=${summary.failed.length}`,
      );
      await db.destroy();
    } catch (err) {
      logger.error(`Trigger rebuild failed: ${err.message}`);
      return false;
    }
  } else logger.warn("[6/7] Skipping trigger rebuild (--skip-triggers)");

  // Phase 7: clear checkpoints
  if (!opts.keepCheckpoint) {
    logger.info(`[7/7] TRUNCATE sym_checkpoint on [${target.id}]`);
    try {
      const db = buildKnexFor(tgtConn);
      await db.raw("TRUNCATE TABLE sym_checkpoint");
      logger.info(`  [${target.id}] sym_checkpoint cleared`);
      await db.destroy();
    } catch (err) {
      if (!/doesn't exist/i.test(err.message)) {
        logger.warn(`  [7/7]: ${err.message}`);
      }
    }
  } else logger.warn("[7/7] Keeping sym_checkpoint (--keep-checkpoint)");

  logger.info(`\n[${target.id}] Reseed complete. Backup: ${backupFile}`);
  return true;
}

async function runDump(conn, tables, outFile) {
  const defaultsFile = makeDefaultsFile(conn);
  const args = [
    `--defaults-extra-file=${defaultsFile}`,
    "--single-transaction",
    "--routines=false",
    "--triggers=false",
    conn.database,
    ...tables,
  ];
  return new Promise((resolve, reject) => {
    const out = fs.createWriteStream(outFile);
    const p = spawnTracked("mysqldump", args, {
      stdio: ["ignore", "pipe", "inherit"],
    });
    p.stdout.pipe(out);
    p.on("close", (code) => {
      try {
        fs.unlinkSync(defaultsFile);
        tempFiles.delete(defaultsFile);
      } catch {
        /* ignore */
      }
      code === 0 ? resolve() : reject(new Error(`mysqldump exit ${code}`));
    });
    p.on("error", reject);
  });
}

async function pipeDump(src, tgt, tables) {
  const srcDefaults = makeDefaultsFile(src);
  const tgtDefaults = makeDefaultsFile(tgt);

  const dumpArgs = [
    `--defaults-extra-file=${srcDefaults}`,
    "--single-transaction",
    "--no-create-info",
    "--routines=false",
    "--triggers=false",
    "--extended-insert",
    "--complete-insert",
    src.database,
    ...tables,
  ];
  const loadArgs = [`--defaults-extra-file=${tgtDefaults}`, tgt.database];

  return new Promise((resolve, reject) => {
    const d = spawnTracked("mysqldump", dumpArgs, {
      stdio: ["ignore", "pipe", "inherit"],
    });
    const l = spawnTracked("mysql", loadArgs, {
      stdio: ["pipe", "inherit", "inherit"],
    });
    d.stdout.pipe(l.stdin);

    const cleanup = () => {
      for (const f of [srcDefaults, tgtDefaults]) {
        try {
          fs.unlinkSync(f);
          tempFiles.delete(f);
        } catch {
          /* ignore */
        }
      }
    };

    let dc = null;
    let lc = null;
    const tryDone = () => {
      if (dc === null || lc === null) return;
      cleanup();
      if (dc !== 0) return reject(new Error(`mysqldump exit ${dc}`));
      if (lc !== 0) return reject(new Error(`mysql exit ${lc}`));
      resolve();
    };
    d.on("close", (c) => {
      dc = c;
      tryDone();
    });
    l.on("close", (c) => {
      lc = c;
      tryDone();
    });
    d.on("error", (err) => {
      cleanup();
      reject(err);
    });
    l.on("error", (err) => {
      cleanup();
      reject(err);
    });
  });
}

/* ------------------------------------------------------------------ */
/* MODE: verify (merge-join, O(1) memory)                            */
/* ------------------------------------------------------------------ */

async function fetchPkBatch(db, table, pkCols, afterCursor, batchSize) {
  const q = db(table).select(pkCols);
  if (afterCursor && pkCols.length === 1) {
    q.where(pkCols[0], ">", afterCursor);
  } else if (afterCursor && pkCols.length > 1) {
    // Composite keyset: (c1, c2, ...) > (v1, v2, ...)
    // Use OR of expanding prefixes — OK for correctness, slightly slow on
    // deep composite keys, but our composite tables are small.
    q.where(function () {
      const cols = pkCols;
      for (let i = 0; i < cols.length; i++) {
        this.orWhere(function () {
          for (let j = 0; j <= i; j++) {
            if (j === i) this.andWhere(cols[j], ">", afterCursor[j]);
            else this.andWhere(cols[j], afterCursor[j]);
          }
        });
      }
    });
  }
  q.orderBy(pkCols.map((c) => ({ column: c, order: "asc" })));
  q.limit(batchSize);
  return q;
}

function cmpPk(a, b, cols) {
  for (let i = 0; i < cols.length; i++) {
    const av = a[cols[i]];
    const bv = b[cols[i]];
    if (av < bv) return -1;
    if (av > bv) return 1;
  }
  return 0;
}

async function mergeDiffPks(hdb, bdb, table, hubPk, brPk) {
  const missingOnBranch = [];
  const missingOnHub = [];
  const BATCH = 5000;

  let hCursor = null;
  let bCursor = null;
  let hBuf = [];
  let bBuf = [];
  let hDone = false;
  let bDone = false;

  while (true) {
    // Refill buffers
    if (hBuf.length === 0 && !hDone) {
      hBuf = await fetchPkBatch(hdb, table, hubPk, hCursor, BATCH);
      if (hBuf.length < BATCH) hDone = true;
      if (hBuf.length > 0) hCursor = hubPk.map((c) => hBuf[hBuf.length - 1][c]);
    }
    if (bBuf.length === 0 && !bDone) {
      bBuf = await fetchPkBatch(bdb, table, brPk, bCursor, BATCH);
      if (bBuf.length < BATCH) bDone = true;
      if (bBuf.length > 0) bCursor = brPk.map((c) => bBuf[bBuf.length - 1][c]);
    }

    if (hBuf.length === 0 && bBuf.length === 0) break;

    // Walk
    let hi = 0;
    let bi = 0;
    while (hi < hBuf.length || bi < bBuf.length) {
      const h = hi < hBuf.length ? hBuf[hi] : null;
      const b = bi < bBuf.length ? bBuf[bi] : null;

      if (!h) {
        // Only b remaining → missing on hub
        for (let k = bi; k < bBuf.length; k++) {
          missingOnHub.push(pkKeyFromRow(bBuf[k], brPk));
        }
        bi = bBuf.length;
      } else if (!b) {
        // Only h remaining → missing on branch
        for (let k = hi; k < hBuf.length; k++) {
          missingOnBranch.push(pkKeyFromRow(hBuf[k], hubPk));
        }
        hi = hBuf.length;
      } else {
        const c = cmpPk(h, b, hubPk);
        if (c === 0) {
          hi++;
          bi++;
        } else if (c < 0) {
          missingOnBranch.push(pkKeyFromRow(h, hubPk));
          hi++;
        } else {
          missingOnHub.push(pkKeyFromRow(b, brPk));
          bi++;
        }
      }
    }

    // Trim consumed portions
    hBuf = [];
    bBuf = [];
  }

  return { missingOnBranch, missingOnHub };
}

async function verify(hub, branches, tables) {
  logger.info(
    `Verifying: hub=[${hub.id}] vs ${branches.length} branch(es), ` +
      `${tables.length} tables (full=${opts.full})\n`,
  );

  let anyMismatch = false;

  for (const branch of branches) {
    logger.info(`--- [${hub.id}] vs [${branch.id}] ---`);
    const hdb = buildKnexFor(parseMysqlUrl(hub.url));
    const bdb = buildKnexFor(parseMysqlUrl(branch.url));

    for (const table of tables) {
      try {
        const hubPk = await getTablePkColumns(
          { id: hub.id, client: "mysql2", db: hdb },
          table,
        );
        const brPk = await getTablePkColumns(
          { id: branch.id, client: "mysql2", db: bdb },
          table,
        );

        if (hubPk.join(",") !== brPk.join(",")) {
          logger.error(
            `[${table}] PK mismatch: hub=[${hubPk}] branch=[${brPk}]`,
          );
          anyMismatch = true;
          continue;
        }

        const hCount = await hdb(table).count("* as c").first();
        const bCount = await bdb(table).count("* as c").first();
        const hN = Number(hCount.c);
        const bN = Number(bCount.c);

        const { missingOnBranch, missingOnHub } = await mergeDiffPks(
          hdb,
          bdb,
          table,
          hubPk,
          brPk,
        );

        let contentDiffs = 0;
        const sampleDiffs = [];

        if (
          opts.full &&
          missingOnBranch.length === 0 &&
          missingOnHub.length === 0 &&
          hN === bN
        ) {
          // Same PK set — compare contents
          const BATCH = 500;
          let cursor = null;
          while (true) {
            const hRows = await fetchPkBatch(hdb, table, hubPk, cursor, BATCH);
            if (hRows.length === 0) break;
            const keys = hRows.map((r) => pkKeyFromRow(r, hubPk));
            let hFull, bFull;
            if (hubPk.length === 1) {
              hFull = await hdb(table).whereIn(
                hubPk[0],
                hRows.map((r) => r[hubPk[0]]),
              );
              bFull = await bdb(table).whereIn(
                brPk[0],
                hRows.map((r) => r[hubPk[0]]),
              );
            } else {
              hFull = hRows; // already all cols? No — need full rows
              hFull = await hdb(table);
              bFull = await bdb(table);
            }
            const bMap = new Map(bFull.map((r) => [pkKeyFromRow(r, brPk), r]));
            for (const h of hFull) {
              const k = pkKeyFromRow(h, hubPk);
              if (!keys.includes(k)) continue;
              const b = bMap.get(k);
              if (!b) continue;
              const diffCols = [];
              for (const col of Object.keys(h)) {
                if (hubPk.includes(col)) continue;
                if (String(h[col]) !== String(b[col])) diffCols.push(col);
              }
              if (diffCols.length > 0) {
                contentDiffs++;
                if (sampleDiffs.length < opts.maxReport) {
                  sampleDiffs.push({ pk: k, cols: diffCols });
                }
              }
            }
            cursor = hubPk.map((c) => hRows[hRows.length - 1][c]);
            if (hRows.length < BATCH) break;
          }
        }

        const bad =
          missingOnBranch.length || missingOnHub.length || contentDiffs;
        if (bad) {
          anyMismatch = true;
          logger.warn(
            `[${table}] hub=${hN} branch=${bN} ` +
              `missingOnBranch=${missingOnBranch.length} missingOnHub=${missingOnHub.length}` +
              (opts.full ? ` contentDiffs=${contentDiffs}` : ""),
          );
          if (missingOnBranch.length) {
            logger.warn(
              `  missingOnBranch sample: ${JSON.stringify(
                missingOnBranch.slice(0, opts.maxReport),
              )}`,
            );
          }
          if (missingOnHub.length) {
            logger.warn(
              `  missingOnHub sample: ${JSON.stringify(
                missingOnHub.slice(0, opts.maxReport),
              )}`,
            );
          }
          if (sampleDiffs.length) {
            logger.warn(
              `  contentDiffs sample: ${JSON.stringify(sampleDiffs)}`,
            );
          }
        } else {
          logger.info(`[${table}] ok (${hN} rows)`);
        }
      } catch (err) {
        logger.error(`[${table}] verify error: ${err.message}`);
        anyMismatch = true;
      }
    }
    await hdb.destroy();
    await bdb.destroy();
  }

  logger.info(
    anyMismatch
      ? "\nResult: MISMATCHES FOUND."
      : "\nResult: all tables in parity.",
  );
  return anyMismatch ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* MODE: fix-logs                                                     */
/* ------------------------------------------------------------------ */

async function fixLogs(nodes) {
  logger.info(
    `Fixing mislabeled sym_change_log entries across ${nodes.length} node(s)\n`,
  );
  const knownIds = new Set(nodes.map((n) => n.id));
  let anyFixed = false;

  for (const node of nodes) {
    const db = buildKnexFor(parseMysqlUrl(node.url));
    try {
      const rows = await db("sym_change_log")
        .select("node_source_id")
        .count("* as cnt")
        .whereNot("node_source_id", node.id)
        .groupBy("node_source_id");

      const mislabeled = rows.filter(
        (r) => knownIds.has(r.node_source_id) && Number(r.cnt) > 0,
      );

      if (mislabeled.length === 0) {
        logger.info(`  [${node.id}] no mislabeled entries`);
        continue;
      }

      for (const r of mislabeled) {
        const cnt = Number(r.cnt);
        if (opts.apply) {
          const n = await db("sym_change_log")
            .where("node_source_id", r.node_source_id)
            .update({ node_source_id: node.id });
          logger.info(
            `  [${node.id}] relabeled ${n} entries: ${r.node_source_id} -> ${node.id}`,
          );
          anyFixed = true;
        } else {
          logger.info(
            `  [${node.id}] (dry-run) would relabel ${cnt} entries: ${r.node_source_id} -> ${node.id}`,
          );
        }
      }

      // Post-verify (in apply mode)
      if (opts.apply) {
        const check = await db("sym_change_log")
          .select("node_source_id")
          .count("* as cnt")
          .whereNot("node_source_id", node.id)
          .groupBy("node_source_id");
        const still = check.filter(
          (r) => knownIds.has(r.node_source_id) && Number(r.cnt) > 0,
        );
        if (still.length > 0) {
          logger.warn(
            `  [${node.id}] WARNING: ${still.length} mislabeled bucket(s) still present after update`,
          );
        }
      }
    } catch (err) {
      logger.error(`  [${node.id}] error: ${err.message}`);
    }
    await db.destroy();
  }

  logger.info(
    anyFixed
      ? "\nAll mislabeled entries fixed."
      : opts.apply
        ? "\nNothing to fix."
        : "\nNothing to fix (dry-run).",
  );
  return 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                               */
/* ------------------------------------------------------------------ */

async function main() {
  const nodes = resolveNodes();
  if (nodes.length === 0) {
    logger.error("No nodes configured (LOCAL_URL / BRANCHES_JSON missing)");
    process.exit(2);
  }

  if (opts.mode === "ping") {
    return await ping(nodes, CHECK_TIMEOUT_MS);
  }

  if (opts.mode === "fix-logs") {
    return await fixLogs(nodes);
  }

  if (opts.mode === "verify") {
    const hub = nodes.find((n) => n.isHub);
    if (!hub) {
      logger.error("No hub node configured (LOCAL_URL missing)");
      process.exit(2);
    }
    const allBranches = nodes.filter((n) => !n.isHub);
    const targets =
      opts.to && opts.to !== "all"
        ? allBranches.filter((b) => b.id === opts.to)
        : allBranches;
    if (targets.length === 0) {
      logger.error(
        `No matching branches. Available: ${allBranches.map((b) => b.id).join(", ")}`,
      );
      process.exit(2);
    }
    const tables = resolveTables();
    return await verify(hub, targets, tables);
  }

  // reseed
  const tables = resolveTables();

  let source;
  if (opts.from) {
    source = nodes.find((n) => n.id === opts.from);
    if (!source) {
      logger.error(
        `Source "${opts.from}" not found. Known: ${nodes.map((n) => n.id).join(", ")}`,
      );
      process.exit(2);
    }
  } else if (opts.to === "all") {
    logger.warn(
      "--from not provided with --to=all — asking interactively for source",
    );
    source = await pickSourceInteractively(nodes);
    opts.from = source.id;
    logger.info(`Selected source: ${source.id}`);
  } else {
    logger.error(
      "--from is required when --to is a specific node (use --to=all for interactive prompt)",
    );
    process.exit(2);
  }

  let targets;
  if (opts.to === "all") {
    targets = nodes.filter((n) => n.id !== source.id);
    if (targets.length === 0) {
      logger.error("--to=all but no other nodes configured");
      process.exit(2);
    }
    logger.info(
      `Copying to ALL other nodes: ${targets.map((t) => t.id).join(", ")}`,
    );
  } else {
    const single = nodes.find((n) => n.id === opts.to);
    if (!single) {
      logger.error(
        `Target "${opts.to}" not found. Known: ${nodes.map((n) => n.id).join(", ")}`,
      );
      process.exit(2);
    }
    if (single.id === source.id) {
      logger.error("Source and target must be different nodes");
      process.exit(2);
    }
    targets = [single];
  }

  if (isPg(source.client) || targets.some((t) => isPg(t.client))) {
    logger.error(
      "This tool only supports MySQL/MariaDB. Postgres users: use pg_dump/psql manually.",
    );
    process.exit(2);
  }

  // Cleanup old backups before generating new ones
  cleanupOldBackups();

  if (!opts.apply) {
    logger.info("");
    logger.info(`Source:  ${source.id}`);
    logger.info(`Targets: ${targets.map((t) => t.id).join(", ")}`);
    logger.info(`Tables (${tables.length}): ${tables.join(", ")}`);
    logger.info("");
    logger.info("[DRY RUN] Would do, per target:");
    logger.info(
      `  1. Back up target's data tables -> ${BACKUP_DIR}/backup-<target>-<ts>.sql`,
    );
    logger.info("  2. Drop sym triggers on target");
    logger.info("  3. Truncate target's data tables");
    logger.info("  4. TRUNCATE sym_change_log on source + target");
    logger.info("  5. mysqldump source | mysql target (data only)");
    logger.info("  6. Rebuild sym triggers on target");
    logger.info("  7. TRUNCATE sym_checkpoint on target");
    logger.info("");
    logger.info("Run with --apply to execute.");
    return 0;
  }

  // Confirm before destructive apply
  if (!(await confirmApply(targets))) {
    logger.warn("Aborted by user.");
    return 2;
  }

  // Pre-flight (skippable for retries)
  if (!opts.skipPreflight) {
    try {
      await preflight(source, targets, tables);
    } catch (err) {
      logger.error(`Pre-flight FAILED: ${err.message}`);
      logger.error("Aborted. Nothing was modified.");
      return 2;
    }
  } else {
    logger.warn("[preflight] SKIPPED (--skip-preflight)");
  }

  logger.warn("Starting in 10 seconds. Ctrl+C to abort...");
  await new Promise((r) => setTimeout(r, 10000));

  const allOk = await reseed(source, targets, tables);

  logger.info("\n=== All targets processed ===");
  if (allOk) {
    logger.info("Restart server.js on all nodes to pick up the new state.");
    return 0;
  }
  logger.error("Some targets failed. Review logs above.");
  return 1;
}

main()
  .then((code) => process.exit(code || 0))
  .catch((err) => {
    logger.error(`[Fatal] ${err.stack || err.message}`);
    cleanupTempFiles();
    process.exit(2);
  });
