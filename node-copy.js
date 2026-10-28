#!/usr/bin/env node
/**
 * node-copy.js — the single ops tool. Modes:
 *
 *   --ping               Reachability check
 *   --verify             Compare hub vs branches (PK + content)
 *   --verify-fk          Compare FK graphs across nodes
 *   --verify-fk-orphans  Scan for orphaned FK references
 *   --fix-logs           Relabel mislabeled sym_change_log entries
 *   --reseed             Full destructive copy (all nodes reset)
 *   --new-branch         Provision a NEW branch from an existing node
 *                        (safe while other branches are live)
 *
 * NEW: --new-branch copies source data (excluding sync tables), sets up
 * the new branch's own store + zero stocks + store_user, sets checkpoint
 * so hub doesn't replay history, and rebuilds triggers. The new store
 * row syncs to hub via the branch's own log. Other branches are untouched.
 *
 * Usage examples:
 *   node node-copy.js --ping
 *   node node-copy.js --verify
 *   node node-copy.js --verify-fk-orphans
 *   node node-copy.js --reseed --from=NODE_MAIN_HUB --to=all --apply
 *   node node-copy.js --new-branch --from=NODE_MAIN_HUB --to=NODE_BRANCH_03 \
 *     --store-name="BRANCH 03" --store-address="Poblacion, Manga" \
 *     --store-contact="0917-123-4567" --store-contact-person="Maria" \
 *     --apply
 *
 * Env (optional):
 *   CHECK_TIMEOUT_MS       Ping timeout (default 3000)
 *   RESEED_TIMEOUT_MS      Dump/load timeout (default 300000)
 *   BACKUP_DIR             Backups dir (default ./backups/)
 *   BACKUP_RETENTION_DAYS  Auto-delete old backups (default 7)
 *
 * Requires: mysqldump and mysql on PATH. MySQL/MariaDB only.
 *
 * Exit: 0 = success; 1 = partial/drift; 2 = fatal/refused
 */

require("dotenv").config();
const knex = require("knex");
const winston = require("winston");
const { spawn, execSync } = require("child_process");
const { URL } = require("url");
const fs = require("fs");
const path = require("path");
const readline = require("readline");
const os = require("os");
const crypto = require("crypto");

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
  enrichTables: null,
  full: true,
  maxReport: 50,
  skipPreflight: false,
  trust: null,
  minScoreDelta: 0.15,
  // new-branch specific
  storeName: null,
  storeAddress: null,
  storeContact: null,
  storeContactPerson: null,
  storeOtherInfo: null,
  storeUuid: null,
  storeUsers: null,
  primaryUser: null,
};

for (const a of argv) {
  if (a === "--apply") opts.apply = true;
  else if (a === "--dry-run") opts.apply = false;
  else if (a === "--skip-triggers") opts.skipTriggers = true;
  else if (a === "--keep-log") opts.keepLog = true;
  else if (a === "--keep-checkpoint") opts.keepCheckpoint = true;
  else if (a === "--no-backup") opts.backup = false;
  else if (a === "--skip-preflight") opts.skipPreflight = true;
  else if (a === "--ping") opts.mode = "ping";
  else if (a === "--verify") opts.mode = "verify";
  else if (a === "--verify-fk") opts.mode = "verify-fk";
  else if (a === "--verify-fk-orphans") opts.mode = "verify-fk-orphans";
  else if (a === "--fix-logs") opts.mode = "fix-logs";
  else if (a === "--reseed") opts.mode = "reseed";
  else if (a === "--new-branch") opts.mode = "new-branch";
  else if (a === "--reconcile") opts.mode = "reconcile";
  else if (a === "--full") opts.full = true;
  else if (a === "--no-full") opts.full = false;
  else if (a.startsWith("--from=")) opts.from = a.slice(7);
  else if (a.startsWith("--to=")) opts.to = a.slice(5);
  else if (a.startsWith("--trust=")) opts.trust = a.slice(8);
  else if (a.startsWith("--tables="))
    opts.tables = a
      .slice(9)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  else if (a.startsWith("--enrich-tables="))
    opts.enrichTables = a
      .slice(16)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  else if (a.startsWith("--max-report="))
    opts.maxReport = parseInt(a.slice(13), 10);
  else if (a.startsWith("--min-score-delta="))
    opts.minScoreDelta = parseFloat(a.slice(18));
  else if (a.startsWith("--store-name=")) opts.storeName = a.slice(13);
  else if (a.startsWith("--store-address=")) opts.storeAddress = a.slice(16);
  else if (a.startsWith("--store-contact=")) opts.storeContact = a.slice(16);
  else if (a.startsWith("--store-contact-person="))
    opts.storeContactPerson = a.slice(23);
  else if (a.startsWith("--store-other-info="))
    opts.storeOtherInfo = a.slice(19);
  else if (a.startsWith("--store-uuid=")) opts.storeUuid = a.slice(13);
  else if (a.startsWith("--store-users="))
    opts.storeUsers = a
      .slice(14)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  else if (a.startsWith("--primary-user=")) opts.primaryUser = a.slice(15);
  else {
    logger.error(`Unknown arg: ${a}`);
    process.exit(2);
  }
}

if (!opts.mode) {
  if (opts.to || opts.from) opts.mode = "reseed";
  else {
    logger.error(
      "Mode required: --ping | --verify | --verify-fk | --verify-fk-orphans | --fix-logs | --reseed | --new-branch | --reconcile",
    );
    process.exit(2);
  }
}

// Validate new-branch args
if (opts.mode === "new-branch") {
  if (!opts.to) {
    logger.error("--new-branch requires --to=<branch_id>");
    process.exit(2);
  }
  if (!opts.storeName) {
    logger.error("--new-branch requires --store-name=<name>");
    process.exit(2);
  }
}

const WRITE_MODES = new Set(["reseed", "reconcile", "fix-logs", "new-branch"]);
const isWriteMode = WRITE_MODES.has(opts.mode);

if (!opts.apply && isWriteMode) {
  logger.info(
    "==================================================================\n" +
      "  DRY RUN MODE — no writes will happen.\n" +
      "  Re-run with --apply to execute.\n" +
      "==================================================================",
  );
} else if (
  opts.apply &&
  (opts.mode === "reseed" ||
    opts.mode === "reconcile" ||
    opts.mode === "new-branch")
) {
  logger.warn(
    "==================================================================\n" +
      "  APPLY MODE — this will modify data.\n" +
      "  Ctrl+C within 10 seconds to abort.\n" +
      "==================================================================",
  );
}

/* ------------------------------------------------------------------ */
/* Signal handling                                                    */
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

function buildNode(conn, id) {
  return { id, client: "mysql2", db: buildKnexFor(conn) };
}

/* ------------------------------------------------------------------ */
/* MySQL defaults file                                                */
/* ------------------------------------------------------------------ */

const tempFiles = new Set();

function makeDefaultsFile(conn) {
  const tmp = path.join(
    os.tmpdir(),
    `.my.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.cnf`,
  );
  fs.writeFileSync(
    tmp,
    `[client]\nhost=${conn.host}\nport=${conn.port}\nuser=${conn.user}\npassword=${conn.password}\n`,
    { mode: 0o600 },
  );
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
  while (true) {
    const answer = await prompt(
      `Which node should be SOURCE (1-${nodes.length})? `,
    );
    const idx = parseInt(answer, 10);
    if (!Number.isNaN(idx) && idx >= 1 && idx <= nodes.length) {
      return nodes[idx - 1];
    }
    logger.warn(`Invalid choice. Enter 1-${nodes.length}.`);
  }
}

async function confirmApply(targets, extraMessage = "") {
  logger.info("");
  logger.info("About to modify data on:");
  targets.forEach((t) => logger.info(`  - ${t.id || t}`));
  if (extraMessage) logger.info(`\n${extraMessage}`);
  logger.info("");
  logger.info("Are you SURE? This is destructive.");
  const answer = await prompt(`Type "yes" to proceed: `);
  return answer.toLowerCase() === "yes";
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
  if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive: true });
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
      if (fs.statSync(full).mtimeMs < cutoff) {
        fs.unlinkSync(full);
        removed++;
      }
    } catch {
      /* ignore */
    }
  }
  if (removed > 0) {
    logger.info(
      `[backups] Removed ${removed} backup(s) older than ${BACKUP_RETENTION_DAYS}d`,
    );
  }
}

/* ------------------------------------------------------------------ */
/* FK-safe order                                                      */
/* ------------------------------------------------------------------ */

async function getDependencyOrder(node, tables) {
  const db = node.db;
  let rawEdges = [];
  try {
    const res = await db.raw(
      `SELECT TABLE_NAME AS child, REFERENCED_TABLE_NAME AS parent
       FROM information_schema.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL`,
    );
    rawEdges = (res[0] || res).map((r) => ({
      child: r.child,
      parent: r.parent,
    }));
  } catch (err) {
    logger.warn(`[table-order] query failed: ${err.message}`);
    return [...tables].sort();
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

  function visit(t) {
    if (visited.has(t) || visiting.has(t)) return;
    visiting.add(t);
    for (const p of parentsOf.get(t) || []) visit(p);
    visiting.delete(t);
    visited.add(t);
    order.push(t);
  }
  for (const t of tables) visit(t);
  for (const t of tables) if (!visited.has(t)) order.push(t);
  return order;
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
    logger.error("No tables to process");
    process.exit(2);
  }
  return tables;
}

/* ------------------------------------------------------------------ */
/* Subprocess helpers                                                 */
/* ------------------------------------------------------------------ */

async function runDumpExcluding(conn, tables, excludeTables, outFile) {
  const defaultsFile = makeDefaultsFile(conn);
  const args = [
    `--defaults-extra-file=${defaultsFile}`,
    "--single-transaction",
    "--routines=false",
    "--triggers=false",
    ...excludeTables.map((t) => `--ignore-table=${conn.database}.${t}`),
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

async function restoreFromFile(conn, dumpFile) {
  const defaultsFile = makeDefaultsFile(conn);
  return new Promise((resolve, reject) => {
    const p = spawnTracked(
      "mysql",
      [`--defaults-extra-file=${defaultsFile}`, conn.database],
      { stdio: ["pipe", "inherit", "inherit"] },
    );
    const input = fs.createReadStream(dumpFile);
    input.pipe(p.stdin);
    p.on("close", (code) => {
      try {
        fs.unlinkSync(defaultsFile);
        tempFiles.delete(defaultsFile);
      } catch {
        /* ignore */
      }
      code === 0 ? resolve() : reject(new Error(`mysql exit ${code}`));
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
    let dc = null,
      lc = null;
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

async function dropTriggersOnTarget(tgtConn) {
  const db = buildKnexFor(tgtConn);
  try {
    const r = await db.raw(
      `SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
       WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME LIKE 'sym_trig_%'`,
    );
    const list = r[0] || r;
    for (const t of list) {
      await db.raw(`DROP TRIGGER IF EXISTS \`${t.TRIGGER_NAME}\``);
    }
    return list.length;
  } finally {
    await db.destroy();
  }
}

/* ------------------------------------------------------------------ */
/* Preflight                                                          */
/* ------------------------------------------------------------------ */

async function preflight(source, targets, tables) {
  logger.info("Pre-flight checks...");
  try {
    execSync("which mysqldump", { stdio: "ignore" });
    execSync("which mysql", { stdio: "ignore" });
  } catch {
    throw new Error("mysqldump or mysql not found on PATH");
  }

  for (const n of [source, ...targets]) {
    const db = buildKnexFor(parseMysqlUrl(n.url));
    try {
      const recent = await db("sym_change_log")
        .where("created_at", ">", new Date(Date.now() - 60000))
        .count("* as c")
        .first();
      if (Number(recent?.c || 0) > 0) {
        throw new Error(
          `[${n.id}] has recent log entries — sync appears RUNNING. Stop pm2 first.`,
        );
      }
    } catch (err) {
      if (/RUNNING/.test(err.message)) throw err;
    } finally {
      await db.destroy().catch(() => {});
    }
  }

  const srcDb = buildKnexFor(parseMysqlUrl(source.url));
  const srcTables = await srcDb.raw(
    `SELECT TABLE_NAME FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'`,
  );
  const srcSet = new Set((srcTables[0] || srcTables).map((r) => r.TABLE_NAME));
  const missingOnSource = tables.filter((t) => !srcSet.has(t));
  if (missingOnSource.length > 0) {
    throw new Error(`Source missing tables: ${missingOnSource.join(", ")}`);
  }
  let totalRows = 0;
  for (const t of tables) {
    try {
      const c = await srcDb(t).count("* as c").first();
      totalRows += Number(c?.c || 0);
    } catch {
      /* ignore */
    }
  }
  if (totalRows === 0) throw new Error("Source has 0 rows — refusing");
  await srcDb.destroy();

  for (const tgt of targets) {
    const tgtDb = buildKnexFor(parseMysqlUrl(tgt.url));
    const tgtTables = await tgtDb.raw(
      `SELECT TABLE_NAME FROM information_schema.TABLES
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'`,
    );
    const tgtSet = new Set(
      (tgtTables[0] || tgtTables).map((r) => r.TABLE_NAME),
    );
    const missing = tables.filter((t) => !tgtSet.has(t));
    if (missing.length > 0) {
      throw new Error(
        `Target [${tgt.id}] missing tables: ${missing.join(", ")}`,
      );
    }
    await tgtDb.destroy();
  }
  logger.info(
    `Pre-flight OK (source ${totalRows} rows, ${tables.length} tables)`,
  );
}

/* ------------------------------------------------------------------ */
/* MODE: ping                                                         */
/* ------------------------------------------------------------------ */

async function ping(nodes) {
  logger.info(
    `Pinging ${nodes.length} node(s) (timeout ${CHECK_TIMEOUT_MS}ms)\n`,
  );
  const results = await Promise.all(
    nodes.map(async (n) => {
      const db = buildKnexFor(parseMysqlUrl(n.url));
      const start = Date.now();
      try {
        await withTimeout(db.raw("SELECT 1"), CHECK_TIMEOUT_MS, "ping");
        return {
          node: n.id,
          type: n.isHub ? "hub" : "branch",
          status: "ONLINE",
          latency: Date.now() - start,
          detail: "",
        };
      } catch (err) {
        return {
          node: n.id,
          type: n.isHub ? "hub" : "branch",
          status: "OFFLINE",
          latency: null,
          detail: String(err.message || err).slice(0, 80),
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
  logger.info(`Summary: ${online}/${results.length} online`);
  return online === results.length ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/* MODE: verify                                                       */
/* ------------------------------------------------------------------ */

async function mergeDiffPks(hdb, bdb, table, hubPk, brPk) {
  const missingOnBranch = [];
  const missingOnHub = [];
  const hRows = await hdb(table)
    .select(hubPk)
    .orderBy(hubPk.map((c) => ({ column: c, order: "asc" })));
  const bRows = await bdb(table)
    .select(brPk)
    .orderBy(brPk.map((c) => ({ column: c, order: "asc" })));

  let hi = 0,
    bi = 0;
  while (hi < hRows.length || bi < bRows.length) {
    const h = hRows[hi];
    const b = bRows[bi];
    if (!h) {
      missingOnHub.push(pkKeyFromRow(b, brPk));
      bi++;
      continue;
    }
    if (!b) {
      missingOnBranch.push(pkKeyFromRow(h, hubPk));
      hi++;
      continue;
    }
    const hk = pkKeyFromRow(h, hubPk);
    const bk = pkKeyFromRow(b, brPk);
    if (hk === bk) {
      hi++;
      bi++;
    } else if (hk < bk) {
      missingOnBranch.push(hk);
      hi++;
    } else {
      missingOnHub.push(bk);
      bi++;
    }
  }
  return { missingOnBranch, missingOnHub };
}

async function verify(hub, branches, tables) {
  logger.info(
    `Verifying: hub=[${hub.id}] vs ${branches.length} branch(es), ${tables.length} tables (full=${opts.full})\n`,
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
          logger.error(`[${table}] PK mismatch`);
          anyMismatch = true;
          continue;
        }

        const hCount = await hdb(table).count("* as c").first();
        const bCount = await bdb(table).count("* as c").first();
        const hN = Number(hCount?.c || 0);
        const bN = Number(bCount?.c || 0);

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
          hN === bN &&
          hN > 0
        ) {
          const BATCH = 500;
          const hSample = await hdb(table).limit(BATCH);
          const keys = hSample.map((r) => r[hubPk[0]]);
          if (keys.length > 0) {
            const hFull = await hdb(table).whereIn(hubPk[0], keys);
            const bFull = await bdb(table).whereIn(brPk[0], keys);
            const bMap = new Map(bFull.map((r) => [r[brPk[0]], r]));
            for (const h of hFull) {
              const b = bMap.get(h[hubPk[0]]);
              if (!b) continue;
              const diffCols = [];
              for (const col of Object.keys(h)) {
                if (hubPk.includes(col)) continue;
                if (String(h[col]) !== String(b[col])) diffCols.push(col);
              }
              if (diffCols.length > 0) {
                contentDiffs++;
                if (sampleDiffs.length < opts.maxReport) {
                  sampleDiffs.push({ pk: h[hubPk[0]], cols: diffCols });
                }
              }
            }
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
          if (missingOnBranch.length)
            logger.warn(
              `  missingOnBranch: ${JSON.stringify(missingOnBranch.slice(0, opts.maxReport))}`,
            );
          if (missingOnHub.length)
            logger.warn(
              `  missingOnHub: ${JSON.stringify(missingOnHub.slice(0, opts.maxReport))}`,
            );
          if (sampleDiffs.length)
            logger.warn(`  contentDiffs: ${JSON.stringify(sampleDiffs)}`);
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
      : "\nResult: all tables in parity ✅",
  );
  return anyMismatch ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* MODE: verify-fk                                                    */
/* ------------------------------------------------------------------ */

async function fetchFkGraph(node) {
  const db = buildKnexFor(parseMysqlUrl(node.url));
  try {
    const res = await db.raw(
      `SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
       FROM information_schema.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL`,
    );
    return (res[0] || res).map(
      (r) =>
        `${r.TABLE_NAME}.${r.COLUMN_NAME} -> ${r.REFERENCED_TABLE_NAME}.${r.REFERENCED_COLUMN_NAME}`,
    );
  } finally {
    await db.destroy();
  }
}

async function verifyFk(nodes) {
  logger.info(`Comparing FK graphs across ${nodes.length} nodes\n`);
  const graphs = await Promise.all(
    nodes.map(async (n) => ({
      id: n.id,
      edges: new Set(await fetchFkGraph(n)),
    })),
  );
  const union = new Set();
  for (const { edges } of graphs) for (const e of edges) union.add(e);

  let drift = false;
  for (const { id, edges } of graphs) {
    const missing = [...union].filter((e) => !edges.has(e));
    const extra = [...edges].filter((e) => !union.has(e));
    logger.info(`[${id}] ${edges.size} FK edge(s)`);
    if (extra.length > 0) {
      drift = true;
      logger.warn(`  EXTRA: ${extra.length}`);
      extra.slice(0, 10).forEach((e) => logger.warn(`    + ${e}`));
    }
    if (missing.length > 0) {
      drift = true;
      logger.warn(`  MISSING: ${missing.length}`);
      missing.slice(0, 10).forEach((e) => logger.warn(`    - ${e}`));
    }
    if (missing.length === 0 && extra.length === 0)
      logger.info(`  ✅ matches cluster`);
  }
  logger.info(
    drift ? "\nResult: FK DRIFT DETECTED" : "\nResult: FK graphs consistent ✅",
  );
  return drift ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* MODE: verify-fk-orphans                                            */
/* ------------------------------------------------------------------ */

async function verifyFkOrphans(nodes) {
  logger.info(`Scanning for FK orphans\n`);
  let totalOrphans = 0;
  for (const node of nodes) {
    logger.info(`[${node.id}]`);
    const db = buildKnexFor(parseMysqlUrl(node.url));
    try {
      const fks = await db.raw(
        `SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
         FROM information_schema.KEY_COLUMN_USAGE
         WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL`,
      );
      let nodeOrphans = 0;
      for (const fk of fks[0] || fks) {
        try {
          const res = await db.raw(
            `SELECT COUNT(*) AS cnt FROM ?? c
             LEFT JOIN ?? p ON p.?? = c.??
             WHERE c.?? IS NOT NULL AND p.?? IS NULL`,
            [
              fk.TABLE_NAME,
              fk.REFERENCED_TABLE_NAME,
              fk.REFERENCED_COLUMN_NAME,
              fk.COLUMN_NAME,
              fk.COLUMN_NAME,
              fk.REFERENCED_COLUMN_NAME,
            ],
          );
          const cnt = Number((res[0] || res)[0]?.cnt || 0);
          if (cnt > 0) {
            logger.warn(
              `  ORPHAN: ${fk.TABLE_NAME}.${fk.COLUMN_NAME} -> ${fk.REFERENCED_TABLE_NAME}.${fk.REFERENCED_COLUMN_NAME} (${cnt} rows)`,
            );
            nodeOrphans += cnt;
          }
        } catch {
          /* ignore */
        }
      }
      if (nodeOrphans === 0) logger.info(`  ✅ No orphans`);
      else {
        logger.warn(`  Total: ${nodeOrphans}`);
        totalOrphans += nodeOrphans;
      }
    } finally {
      await db.destroy();
    }
  }
  logger.info(
    totalOrphans === 0
      ? "\nResult: no FK orphans ✅"
      : `\nResult: ${totalOrphans} FK orphans`,
  );
  return totalOrphans === 0 ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/* MODE: fix-logs                                                     */
/* ------------------------------------------------------------------ */

async function fixLogs(nodes) {
  logger.info(
    `Fixing mislabeled sym_change_log across ${nodes.length} node(s)\n`,
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
            `  [${node.id}] relabeled ${n}: ${r.node_source_id} -> ${node.id}`,
          );
          anyFixed = true;
        } else {
          logger.info(
            `  [${node.id}] would relabel ${cnt}: ${r.node_source_id} -> ${node.id}`,
          );
        }
      }
    } catch (err) {
      logger.error(`  [${node.id}] ${err.message}`);
    }
    await db.destroy();
  }

  logger.info(
    anyFixed
      ? "\nDone."
      : opts.apply
        ? "\nNothing to fix."
        : "\nNothing to fix (dry-run).",
  );
  return 0;
}

/* ------------------------------------------------------------------ */
/* MODE: reseed                                                       */
/* ------------------------------------------------------------------ */

async function reseedOne(source, target, tables) {
  const srcConn = parseMysqlUrl(source.url);
  const tgtConn = parseMysqlUrl(target.url);
  const backupFile = uniqueBackupPath(target.id);

  if (opts.backup) {
    logger.info(`[1/7] Backing up [${target.id}] -> ${backupFile}`);
    try {
      await withTimeout(
        runDumpExcluding(tgtConn, tables, [], backupFile),
        RESEED_TIMEOUT_MS,
        "backup",
      );
      const size = (fs.statSync(backupFile).size / 1024 / 1024).toFixed(1);
      logger.info(`[1/7] Backup written (${size} MB)`);
    } catch (err) {
      logger.error(`Backup failed: ${err.message}`);
      return false;
    }
  }

  if (!opts.skipTriggers) {
    logger.info(`[2/7] Dropping triggers on [${target.id}]`);
    try {
      const count = await dropTriggersOnTarget(tgtConn);
      logger.info(`[2/7] Dropped ${count} triggers`);
    } catch (err) {
      logger.error(`Trigger drop failed: ${err.message}`);
      return false;
    }
  }

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
    logger.info(`[3/7] Truncated ${truncated}/${tables.length}`);
  } catch (err) {
    logger.error(`Truncate failed: ${err.message}`);
    return false;
  }

  if (!opts.keepLog) {
    logger.info(
      `[4/7] TRUNCATE sym_change_log on [${source.id}] and [${target.id}]`,
    );
    for (const n of [source, target]) {
      try {
        const db = buildKnexFor(parseMysqlUrl(n.url));
        await db.raw("TRUNCATE TABLE sym_change_log");
        logger.info(`  [${n.id}] cleared`);
        await db.destroy();
      } catch (err) {
        if (!/doesn't exist/i.test(err.message))
          logger.warn(`  [${n.id}]: ${err.message}`);
      }
    }
  }

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
    logger.error(`Restore from ${backupFile} if needed.`);
    return false;
  }

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
  }

  if (!opts.keepCheckpoint) {
    logger.info(`[7/7] TRUNCATE sym_checkpoint on [${target.id}]`);
    try {
      const db = buildKnexFor(tgtConn);
      await db.raw("TRUNCATE TABLE sym_checkpoint");
      logger.info(`  cleared`);
      await db.destroy();
    } catch (err) {
      if (!/doesn't exist/i.test(err.message))
        logger.warn(`  [7/7]: ${err.message}`);
    }
  }

  logger.info(`\n[${target.id}] Reseed complete. Backup: ${backupFile}`);
  return true;
}

async function reseed(source, targets, tables) {
  const results = await Promise.all(
    targets.map(async (tgt) => {
      logger.info(`\n========== Reseed ${source.id} -> ${tgt.id} ==========`);
      try {
        return { target: tgt.id, ok: await reseedOne(source, tgt, tables) };
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

/* ------------------------------------------------------------------ */
/* MODE: new-branch                                                   */
/* ------------------------------------------------------------------ */

async function provisionNewBranch(source, target, newStoreMeta, tables) {
  const srcConn = parseMysqlUrl(source.url);
  const tgtConn = parseMysqlUrl(target.url);

  // Step 0: Backup existing target (in case it's not truly fresh)
  const targetBackup = uniqueBackupPath(`${target.id}-pre-new-branch`);
  logger.info(`[0/7] Backing up existing [${target.id}] -> ${targetBackup}`);
  try {
    await runDumpExcluding(tgtConn, tables, [], targetBackup);
    logger.info(`[0/7] Target backup written`);
  } catch (err) {
    logger.warn(`[0/7] Target backup failed (may be empty): ${err.message}`);
  }

  // Step 0b: Record source max change_id (BEFORE dump)
  const srcDb = buildKnexFor(srcConn);
  let maxChangeId = 0n;
  try {
    const maxRow = await srcDb("sym_change_log").max("change_id as m").first();
    maxChangeId = BigInt(maxRow?.m || 0);
  } catch (err) {
    if (!/doesn't exist/i.test(err.message)) throw err;
  }
  await srcDb.destroy();
  logger.info(`[0/7] Source [${source.id}] max change_id = ${maxChangeId}`);

  // Step 1: Dump source (excluding sync tables)
  const dumpFile = path.join(BACKUP_DIR, `new-branch-source-${Date.now()}.sql`);
  logger.info(`[1/7] Dumping [${source.id}] -> ${dumpFile}`);
  await withTimeout(
    runDumpExcluding(
      srcConn,
      tables,
      ["sym_change_log", "sym_checkpoint"],
      dumpFile,
    ),
    RESEED_TIMEOUT_MS,
    "dump",
  );
  const dumpSize = (fs.statSync(dumpFile).size / 1024 / 1024).toFixed(1);
  logger.info(`[1/7] Dump written (${dumpSize} MB)`);

  // Step 2: Drop triggers on target
  logger.info(`[2/7] Dropping triggers on [${target.id}]`);
  const droppedCount = await dropTriggersOnTarget(tgtConn);
  logger.info(`[2/7] Dropped ${droppedCount} triggers`);

  // Step 3: Truncate target tables + sync state
  logger.info(
    `[3/7] Truncating ${tables.length} tables + sync state on [${target.id}]`,
  );
  const tgtDb1 = buildKnexFor(tgtConn);
  await tgtDb1.raw("SET FOREIGN_KEY_CHECKS = 0");
  let truncated = 0;
  for (const tbl of tables) {
    try {
      await tgtDb1.raw(`TRUNCATE TABLE \`${tbl}\``);
      truncated++;
    } catch (err) {
      logger.warn(`  truncate ${tbl}: ${err.message}`);
    }
  }
  for (const t of ["sym_change_log", "sym_checkpoint"]) {
    try {
      await tgtDb1.raw(`TRUNCATE TABLE \`${t}\``);
    } catch (err) {
      if (!/doesn't exist/i.test(err.message))
        logger.warn(`  truncate ${t}: ${err.message}`);
    }
  }
  await tgtDb1.raw("SET FOREIGN_KEY_CHECKS = 1");
  await tgtDb1.destroy();
  logger.info(`[3/7] Truncated ${truncated}/${tables.length} tables`);

  // Step 3b: Restore source dump into target
  logger.info(`[3/7] Restoring source dump into [${target.id}]`);
  await withTimeout(
    restoreFromFile(tgtConn, dumpFile),
    RESEED_TIMEOUT_MS,
    "restore",
  );
  logger.info(`[3/7] Restore complete`);

  // Step 4: Clean inherited operational state
  logger.info(`[4/7] Cleaning inherited operational state on [${target.id}]`);
  const cleanDb = buildKnexFor(tgtConn);
  const openSessions = await cleanDb("pos_session").where("status", 1).del();
  const todayStart = new Date();
  todayStart.setUTCHours(0, 0, 0, 0);
  const todaysCash = await cleanDb("cash_count")
    .where("created_at", ">=", todayStart)
    .del();
  logger.info(
    `[4/7] Cleared ${openSessions} open pos_session(s), ${todaysCash} today's cash_count(s)`,
  );
  await cleanDb.destroy();

  // Step 5: Rebuild triggers BEFORE creating target-specific data
  // (so store/stocks/store_user inserts get logged and can sync to hub)
  logger.info(`[5/7] Rebuilding triggers on [${target.id}]`);
  const targetNode = {
    id: target.id,
    client: "mysql2",
    db: buildKnexFor(tgtConn),
  };
  const trigSummary = await setupNodeTriggers(targetNode);
  await targetNode.db.destroy();
  logger.info(
    `[5/7] Triggers rebuilt: ok=${trigSummary.ok.length} skipped=${trigSummary.skipped.length} failed=${trigSummary.failed.length}`,
  );

  // Step 6: Create target-specific data (store, stocks, store_user)
  logger.info(`[6/7] Creating target-specific data for [${target.id}]`);
  const setupDb = buildKnexFor(tgtConn);
  const newStoreUuid = newStoreMeta.storeUuid || crypto.randomUUID();

  // Check if store name already exists
  const existing = await setupDb("store")
    .where("name", newStoreMeta.name)
    .first();
  if (existing) {
    await setupDb.destroy();
    throw new Error(
      `Store "${newStoreMeta.name}" already exists on [${target.id}] with record_id=${existing.record_id}`,
    );
  }

  await setupDb("store").insert({
    record_id: newStoreUuid,
    name: newStoreMeta.name,
    address: newStoreMeta.address || "",
    contact_no: newStoreMeta.contactNo || "",
    contact_person: newStoreMeta.contactPerson || "",
    other_info: newStoreMeta.otherInfo || "",
    type: 0,
    setup: null,
    status: 1,
    created_at: new Date(),
    updated_at: new Date(),
  });
  logger.info(`[6/7] Store "${newStoreMeta.name}" created (${newStoreUuid})`);

  const products = await setupDb("product")
    .select("record_id")
    .where("status", 1);
  let stockCount = 0;
  for (let i = 0; i < products.length; i += 100) {
    const chunk = products.slice(i, i + 100);
    const rows = chunk.map((p) => ({
      record_id: crypto.randomUUID(),
      product: p.record_id,
      store: newStoreUuid,
      qty: 0,
      status: 1,
      last_updated: null,
      created_at: new Date(),
      updated_at: new Date(),
    }));
    await setupDb("stocks_on_hand").insert(rows);
    stockCount += rows.length;
  }
  logger.info(
    `[6/7] Created ${stockCount} zero-stock rows for ${products.length} product(s)`,
  );

  if (newStoreMeta.userIds && newStoreMeta.userIds.length > 0) {
    let userCount = 0;
    for (const userId of newStoreMeta.userIds) {
      const exists = await setupDb("store_user")
        .where({ user: userId, store: newStoreUuid })
        .first();
      if (exists) {
        logger.debug(`  user ${userId} already assigned — skip`);
        continue;
      }
      await setupDb("store_user").insert({
        record_id: crypto.randomUUID(),
        user: userId,
        store: newStoreUuid,
        is_primary: userId === newStoreMeta.primaryUserId ? 1 : 0,
        created_at: new Date(),
        updated_at: new Date(),
      });
      userCount++;
    }
    logger.info(`[6/7] Assigned ${userCount} user(s) to store`);
  } else {
    logger.warn(`[6/7] No users specified — store has no user assignments`);
  }

  // Step 7: Set checkpoint so hub doesn't replay history
  const buffer = 10000n;
  const startCheckpoint = maxChangeId > buffer ? maxChangeId - buffer : 0n;
  logger.info(
    `[7/7] Setting checkpoint for [${source.id}] to ${startCheckpoint} (${buffer} buffer)`,
  );
  await setupDb("sym_checkpoint").insert({
    node_id: source.id,
    last_processed_change_id: startCheckpoint.toString(),
    updated_at: new Date(),
  });
  await setupDb.destroy();

  logger.info(`\n✅ [${target.id}] provisioned successfully`);
  logger.info(`   Store:      ${newStoreMeta.name} (${newStoreUuid})`);
  logger.info(`   Products:   ${products.length}`);
  logger.info(`   Stocks:     ${stockCount} zero-qty rows`);
  logger.info(`   Users:      ${newStoreMeta.userIds?.length || 0}`);
  logger.info(
    `   Checkpoint: ${startCheckpoint} → will sync forward from here`,
  );
  logger.info(`\n   Next: pm2 start all (on all nodes including new branch)`);
  logger.info(
    `   The store/stocks/store_user rows are in target's log and will sync to hub.`,
  );

  return true;
}

/* ------------------------------------------------------------------ */
/* MODE: reconcile                                                    */
/* ------------------------------------------------------------------ */

async function scoreNode(node, tables) {
  const db = buildKnexFor(parseMysqlUrl(node.url));
  let totalRows = 0;
  let orphanCount = 0;

  try {
    for (const t of tables) {
      try {
        const c = await db(t).count("* as c").first();
        totalRows += Number(c?.c || 0);
      } catch {
        /* ignore */
      }
    }
    const fks = await db.raw(
      `SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
       FROM information_schema.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL`,
    );
    for (const fk of fks[0] || fks) {
      if (!tables.includes(fk.TABLE_NAME)) continue;
      try {
        const res = await db.raw(
          `SELECT COUNT(*) as cnt FROM ?? c
           LEFT JOIN ?? p ON p.?? = c.??
           WHERE c.?? IS NOT NULL AND p.?? IS NULL`,
          [
            fk.TABLE_NAME,
            fk.REFERENCED_TABLE_NAME,
            fk.REFERENCED_COLUMN_NAME,
            fk.COLUMN_NAME,
            fk.COLUMN_NAME,
            fk.REFERENCED_COLUMN_NAME,
          ],
        );
        orphanCount += Number((res[0] || res)[0]?.cnt || 0);
      } catch {
        /* ignore */
      }
    }
  } finally {
    await db.destroy();
  }

  return {
    id: node.id,
    totalRows,
    orphanCount,
    score: totalRows - orphanCount * 100,
  };
}

async function reconcile(nodes, tables) {
  logger.info(`Reconcile: scoring ${nodes.length} node(s)\n`);
  const scores = await Promise.all(nodes.map((n) => scoreNode(n, tables)));
  scores.sort((a, b) => b.score - a.score);

  logger.info("Scores (higher = healthier):");
  console.table(
    scores.map((s) => ({
      node: s.id,
      rows: s.totalRows,
      orphans: s.orphanCount,
      score: Math.round(s.score),
    })),
  );

  let source;
  if (opts.trust) {
    const wanted =
      opts.trust === "hub" ? nodes.find((n) => n.isHub)?.id : opts.trust;
    source = nodes.find((n) => n.id === wanted);
    if (!source) {
      logger.error(`--trust=${opts.trust} — not found`);
      return 2;
    }
    logger.info(`\nForced source: [${source.id}]`);
  } else {
    const best = scores[0];
    const runner = scores[1];
    if (runner && best.score > 0) {
      const delta = (best.score - runner.score) / Math.abs(best.score);
      if (delta < opts.minScoreDelta) {
        logger.error(
          `\nTop two too close (${best.id}: ${Math.round(best.score)} vs ${runner.id}: ${Math.round(runner.score)}). ` +
            `Use --trust=<id> to force.`,
        );
        return 2;
      }
    }
    source = nodes.find((n) => n.id === best.id);
    logger.info(
      `\nAuto-picked source: [${source.id}] (score ${Math.round(best.score)})`,
    );
  }

  const otherBranches = nodes.filter((n) => n.id !== source.id);

  if (!opts.apply) {
    logger.info(`\n[DRY RUN] Would reseed all branches from [${source.id}].`);
    if (opts.enrichTables && opts.enrichTables.length > 0) {
      logger.info(`Enrich tables: ${opts.enrichTables.join(", ")}`);
    }
    return 0;
  }

  if (
    !(await confirmApply(otherBranches, `Will reseed from [${source.id}].`))
  ) {
    logger.warn("Aborted.");
    return 2;
  }

  if (opts.enrichTables && opts.enrichTables.length > 0) {
    logger.info(
      `\n[Enrich] Pulling unique rows for: ${opts.enrichTables.join(", ")}`,
    );
    const srcConn = parseMysqlUrl(source.url);
    const srcNode = buildNode(srcConn, source.id);
    for (const branch of otherBranches) {
      const brConn = parseMysqlUrl(branch.url);
      const brNode = buildNode(brConn, branch.id);
      for (const t of opts.enrichTables) {
        if (!tables.includes(t)) continue;
        try {
          const pk = await getTablePkColumns(brNode, t);
          const brRows = await brNode.db(t).select("*");
          const srcPkRows = await srcNode.db(t).select(pk);
          const srcSet = new Set(srcPkRows.map((r) => pkKeyFromRow(r, pk)));
          const unique = brRows.filter((r) => !srcSet.has(pkKeyFromRow(r, pk)));
          if (unique.length > 0) {
            let inserted = 0;
            for (let i = 0; i < unique.length; i += 100) {
              try {
                await srcNode
                  .db(t)
                  .insert(unique.slice(i, i + 100))
                  .onConflict(pk)
                  .ignore();
                inserted += Math.min(100, unique.length - i);
              } catch {
                for (const row of unique.slice(i, i + 100)) {
                  try {
                    await srcNode.db(t).insert(row).onConflict(pk).ignore();
                    inserted++;
                  } catch {
                    /* skip */
                  }
                }
              }
            }
            logger.info(`  [${branch.id}] ${t}: +${inserted} rows into source`);
          }
        } catch (err) {
          logger.warn(`  [${branch.id}] ${t}: enrich failed: ${err.message}`);
        }
      }
      await brNode.db.destroy();
    }
    await srcNode.db.destroy();
  }

  logger.info(
    `\n[Reseed] ${source.id} -> ${otherBranches.map((b) => b.id).join(", ")}`,
  );
  const ok = await reseed(source, otherBranches, tables);
  return ok ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/* Main                                                               */
/* ------------------------------------------------------------------ */

async function main() {
  const nodes = resolveNodes();
  if (nodes.length === 0) {
    logger.error("No nodes configured");
    process.exit(2);
  }

  if (opts.mode === "ping") return await ping(nodes);
  if (opts.mode === "fix-logs") return await fixLogs(nodes);
  if (opts.mode === "verify-fk") return await verifyFk(nodes);
  if (opts.mode === "verify-fk-orphans") return await verifyFkOrphans(nodes);

  if (opts.mode === "verify") {
    const hub = nodes.find((n) => n.isHub);
    if (!hub) {
      logger.error("No hub configured");
      process.exit(2);
    }
    const allBranches = nodes.filter((n) => !n.isHub);
    const targets =
      opts.to && opts.to !== "all"
        ? allBranches.filter((b) => b.id === opts.to)
        : allBranches;
    if (targets.length === 0) {
      logger.error(`No matching branches`);
      process.exit(2);
    }
    return await verify(hub, targets, resolveTables());
  }

  if (opts.mode === "reconcile") {
    return await reconcile(nodes, resolveTables());
  }

  // --new-branch or --reseed
  const tables = resolveTables();

  // Resolve source (defaults to hub for new-branch)
  let source;
  if (opts.from) {
    source = nodes.find((n) => n.id === opts.from);
    if (!source) {
      logger.error(`Source "${opts.from}" not found`);
      process.exit(2);
    }
  } else if (opts.mode === "new-branch") {
    // Default source for new-branch is hub
    source = nodes.find((n) => n.isHub);
    if (!source) {
      logger.error("No hub found and --from not specified");
      process.exit(2);
    }
  } else if (opts.to === "all") {
    source = await pickSourceInteractively(nodes);
    opts.from = source.id;
  } else {
    logger.error("--from required (or --to=all for interactive prompt)");
    process.exit(2);
  }

  // Resolve target(s)
  let targets;
  if (opts.mode === "new-branch") {
    const single = nodes.find((n) => n.id === opts.to);
    if (!single) {
      logger.error(
        `Target "${opts.to}" not found. Add it to BRANCHES_JSON first.`,
      );
      process.exit(2);
    }
    if (single.id === source.id) {
      logger.error("Source and target must be different");
      process.exit(2);
    }
    targets = [single];
  } else if (opts.to === "all") {
    targets = nodes.filter((n) => n.id !== source.id);
  } else {
    const single = nodes.find((n) => n.id === opts.to);
    if (!single || single.id === source.id) {
      logger.error("Invalid target");
      process.exit(2);
    }
    targets = [single];
  }

  if (isPg(source.client) || targets.some((t) => isPg(t.client))) {
    logger.error("MySQL/MariaDB only");
    process.exit(2);
  }

  cleanupOldBackups();

  // === new-branch mode ===
  if (opts.mode === "new-branch") {
    const newStoreMeta = {
      name: opts.storeName,
      address: opts.storeAddress,
      contactNo: opts.storeContact,
      contactPerson: opts.storeContactPerson,
      otherInfo: opts.storeOtherInfo,
      storeUuid: opts.storeUuid,
      userIds: opts.storeUsers,
      primaryUserId: opts.primaryUser,
    };

    if (!opts.apply) {
      logger.info(
        `\n[DRY RUN] Would provision new branch [${opts.to}] from [${source.id}]`,
      );
      logger.info(`   Store:      ${newStoreMeta.name}`);
      logger.info(`   Address:    ${newStoreMeta.address || "(empty)"}`);
      logger.info(`   Contact:    ${newStoreMeta.contactNo || "(empty)"}`);
      logger.info(
        `   Contact person: ${newStoreMeta.contactPerson || "(empty)"}`,
      );
      logger.info(
        `   Users:      ${newStoreMeta.userIds?.join(", ") || "(none)"}`,
      );
      logger.info(`   Primary:    ${newStoreMeta.primaryUserId || "(none)"}`);
      logger.info(`\n   Steps:`);
      logger.info(`     1. Backup existing target`);
      logger.info(
        `     2. Dump [${source.id}] excluding sym_change_log/sym_checkpoint`,
      );
      logger.info(`     3. Drop triggers on [${opts.to}]`);
      logger.info(`     4. Truncate all tables on [${opts.to}]`);
      logger.info(`     5. Restore source dump into [${opts.to}]`);
      logger.info(`     6. Clear inherited pos_session/cash_count`);
      logger.info(`     7. Rebuild triggers on [${opts.to}]`);
      logger.info(
        `     8. Create new store + zero stocks + store_user assignments`,
      );
      logger.info(`     9. Set checkpoint so hub doesn't replay history`);
      logger.info(`\n   Run with --apply to execute.`);
      return 0;
    }

    if (
      !(await confirmApply(
        [opts.to],
        `Source: [${source.id}], Store: "${newStoreMeta.name}"`,
      ))
    ) {
      logger.warn("Aborted.");
      return 2;
    }

    if (!opts.skipPreflight) {
      try {
        await preflight(source, targets, tables);
      } catch (err) {
        logger.error(`Pre-flight FAILED: ${err.message}`);
        return 2;
      }
    }

    logger.warn("Starting in 10 seconds. Ctrl+C to abort...");
    await new Promise((r) => setTimeout(r, 10000));

    try {
      await provisionNewBranch(source, targets[0], newStoreMeta, tables);
      return 0;
    } catch (err) {
      logger.error(`New-branch provisioning failed: ${err.message}`);
      logger.error(`Stack: ${err.stack}`);
      return 1;
    }
  }

  // === reseed mode (existing) ===
  if (!opts.apply) {
    logger.info(`\nSource: ${source.id}`);
    logger.info(`Targets: ${targets.map((t) => t.id).join(", ")}`);
    logger.info(`Tables (${tables.length})`);
    logger.info(
      `\n[DRY RUN] Would backup, drop triggers, truncate, copy, rebuild, reset.`,
    );
    logger.info(`Run with --apply to execute.`);
    return 0;
  }

  if (!(await confirmApply(targets))) {
    logger.warn("Aborted.");
    return 2;
  }

  if (!opts.skipPreflight) {
    try {
      await preflight(source, targets, tables);
    } catch (err) {
      logger.error(`Pre-flight FAILED: ${err.message}`);
      return 2;
    }
  }

  logger.warn("Starting in 10 seconds. Ctrl+C to abort...");
  await new Promise((r) => setTimeout(r, 10000));

  const ok = await reseed(source, targets, tables);
  logger.info(
    ok
      ? "\n✅ Reseed complete. Restart server.js on all nodes."
      : "\n⚠️ Some targets failed.",
  );
  return ok ? 0 : 1;
}

main()
  .then((code) => process.exit(code || 0))
  .catch((err) => {
    logger.error(`[Fatal] ${err.stack || err.message}`);
    cleanupTempFiles();
    process.exit(2);
  });
