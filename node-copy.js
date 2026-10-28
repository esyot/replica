#!/usr/bin/env node
/**
 * node-copy.js — the single ops tool. Modes:
 *
 *   --ping               Reachability check (SELECT 1 with timeout)
 *   --verify             Compare hub vs branches (PK + content). Read-only.
 *   --verify-fk          Compare FK graphs across nodes. Read-only.
 *   --verify-fk-orphans  Scan for orphaned FK references. Read-only.
 *   --fix-logs           Relabel mislabeled sym_change_log entries.
 *   --reseed             Copy one node's data to one or all others (destructive).
 *   --reconcile          Score nodes, pick healthiest, reseed (destructive).
 *
 * Common flags:
 *   --from=ID --to=ID|all    For --reseed: source and target(s)
 *   --trust=ID|hub           For --reconcile: force source (skip auto-pick)
 *   --apply                  Actually write (default is dry-run)
 *   --tables=t1,t2           Restrict to specific tables
 *   --enrich-tables=t1,t2    For --reconcile: pull unique rows from these tables only
 *   --skip-triggers          Skip drop/rebuild triggers
 *   --keep-log               Don't truncate sym_change_log after reseed
 *   --keep-checkpoint        Don't truncate sym_checkpoint after reseed
 *   --no-backup              Skip backup phase
 *   --skip-preflight         Skip safety pre-checks
 *   --full / --no-full       For --verify: compare row content (default: on)
 *   --max-report=N           For --verify: cap samples (default 50)
 *
 * Env:
 *   CHECK_TIMEOUT_MS         Ping timeout per node (default 3000)
 *   RESEED_TIMEOUT_MS        Dump/load phase timeout (default 300000)
 *   BACKUP_DIR               Backups location (default ./backups/)
 *   BACKUP_RETENTION_DAYS    Auto-delete older backups (default 7)
 *
 * Requires: mysqldump and mysql on PATH. MySQL/MariaDB only.
 *
 * Exit: 0 = success / clean / all online; 1 = partial / drift found; 2 = fatal
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
  else {
    logger.error(`Unknown arg: ${a}`);
    process.exit(2);
  }
}

if (!opts.mode) {
  if (opts.to || opts.from) opts.mode = "reseed";
  else {
    logger.error(
      "Mode required: --ping | --verify | --verify-fk | --verify-fk-orphans | --fix-logs | --reseed | --reconcile",
    );
    process.exit(2);
  }
}

const WRITE_MODES = new Set(["reseed", "reconcile", "fix-logs"]);
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
  (opts.mode === "reseed" || opts.mode === "reconcile")
) {
  logger.warn(
    "==================================================================\n" +
      "  APPLY MODE — this will WIPE the target node(s).\n" +
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
/* MySQL defaults file (passwords not on CLI)                        */
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
      `Which node should be the SOURCE (1-${nodes.length})? `,
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
  logger.info("About to WIPE data tables on:");
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
/* FK-safe order (queries information_schema)                        */
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
  const offline = results.length - online;
  logger.info(
    `Summary: ${online}/${results.length} online, ${offline} offline`,
  );
  return offline > 0 ? 1 : 0;
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
          /* ignore per-FK errors */
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
          `[${n.id}] has recent log entries — sync is RUNNING. Stop pm2 first.`,
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

async function reseedOne(source, target, tables) {
  const srcConn = parseMysqlUrl(source.url);
  const tgtConn = parseMysqlUrl(target.url);
  const backupFile = uniqueBackupPath(target.id);

  if (target.id === process.env.LOCAL_ID) {
    logger.warn(`Target [${target.id}] is THIS machine`);
  }

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
  }

  if (!opts.skipTriggers) {
    logger.info(`[2/7] Dropping triggers on [${target.id}]`);
    try {
      const db = buildKnexFor(tgtConn);
      const r = await db.raw(
        `SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
         WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME LIKE 'sym_trig_%'`,
      );
      const list = r[0] || r;
      for (const t of list)
        await db.raw(`DROP TRIGGER IF EXISTS \`${t.TRIGGER_NAME}\``);
      logger.info(`[2/7] Dropped ${list.length} triggers`);
      await db.destroy();
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

  // Dry-run report
  if (!opts.apply) {
    logger.info(`\n[DRY RUN] Would:`);
    if (opts.enrichTables && opts.enrichTables.length > 0) {
      logger.info(
        `  1. Enrich [${source.id}] from these tables in other nodes:`,
      );
      logger.info(`     ${opts.enrichTables.join(", ")}`);
    } else {
      logger.info(
        `  1. (Skip enrichment — use --enrich-tables=t1,t2 to enable)`,
      );
    }
    logger.info(`  2. Reseed all branches from [${source.id}]`);
    logger.info(`\nRun with --apply to execute.`);
    return 0;
  }

  if (
    !(await confirmApply(otherBranches, `Will reseed from [${source.id}].`))
  ) {
    logger.warn("Aborted.");
    return 2;
  }

  // Enrichment: pull unique rows from specific tables only
  if (opts.enrichTables && opts.enrichTables.length > 0) {
    logger.info(
      `\n[Enrich] Pulling unique rows for: ${opts.enrichTables.join(", ")}`,
    );
    const srcConn = parseMysqlUrl(source.url);
    const srcNode = buildNode(srcConn, source.id);
    // Pull unique rows from each other branch, insert into source
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
                /* per-row fallback */
                for (const row of unique.slice(i, i + 100)) {
                  try {
                    await srcNode.db(t).insert(row).onConflict(pk).ignore();
                    inserted++;
                  } catch {
                    /* skip — orphan */
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

  // Reseed
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

  // reseed
  const tables = resolveTables();
  let source;
  if (opts.from) {
    source = nodes.find((n) => n.id === opts.from);
    if (!source) {
      logger.error(`Source "${opts.from}" not found`);
      process.exit(2);
    }
  } else if (opts.to === "all") {
    source = await pickSourceInteractively(nodes);
    opts.from = source.id;
  } else {
    logger.error("--from required (or --to=all for interactive prompt)");
    process.exit(2);
  }

  let targets;
  if (opts.to === "all") {
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
