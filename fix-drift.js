#!/usr/bin/env node
/**
 * fix-drift.js — dynamic reconciler for the replica sync fabric.
 *
 * Detects and (with --apply) fixes row-set drift between hub and branches.
 * Insert-only. Never deletes. Unique-index collisions are reported, never
 * silently dropped. Content diffs are reported, never auto-applied unless
 * --apply-content and a per-table authority policy are both set.
 *
 * Usage:
 *   node fix-drift.js                          # dry-run, prints plan
 *   node fix-drift.js --apply                  # apply row-set inserts
 *   node fix-drift.js --apply --collision=null # null colliding codes instead of prefix
 *   node fix-drift.js --apply --collision=skip # leave colliding rows alone
 *   node fix-drift.js --apply-content --content-authority=deposit:NODE_BRANCH_02
 *   node fix-drift.js --verbose                # full collision lists
 *   node fix-drift.js --tables=customer,repa_deposit  # limit scope
 *
 * Exit codes: 0 = parity reached, 1 = dry-run (plan printed), 2 = error, 3 = still drifted after apply.
 */
"use strict";

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");
const mysql = require("mysql2/promise");

// ---------------------------------------------------------------- env / CLI

// Minimal .env parser — matches the single-line discipline the project relies on.
function loadEnv(file) {
  const txt = fs.readFileSync(file, "utf8");
  const env = {};
  for (const line of txt.split(/\r?\n/)) {
    if (!line || line.startsWith("#")) continue;
    const m = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (!m) continue;
    let v = m[2];
    if (
      (v.startsWith('"') && v.endsWith('"')) ||
      (v.startsWith("'") && v.endsWith("'"))
    ) {
      v = v.slice(1, -1);
    }
    env[m[1]] = v;
  }
  return env;
}

const ENV = loadEnv(path.join(__dirname, ".env"));

function parseConnUrl(urlStr) {
  const u = new URL(urlStr);
  return {
    host: u.hostname,
    port: parseInt(u.port || "3306", 10),
    user: decodeURIComponent(u.username),
    password: decodeURIComponent(u.password),
    database: u.pathname.replace(/^\//, ""),
  };
}

const HUB_CONN = {
  id: "NODE_MAIN_HUB",
  cfg: {
    host: ENV.HUB_HOST || "127.0.0.1",
    port: parseInt(ENV.HUB_PORT || "3306", 10),
    user: ENV.HUB_USER || "root",
    password: ENV.HUB_PASSWORD || "candgtrading@2026",
    database: ENV.HUB_DB || "candg_pos_db",
  },
};

const BRANCH_CONNS = JSON.parse(ENV.BRANCHES_JSON || "[]").map((b) => ({
  id: b.id,
  cfg: parseConnUrl(b.url),
}));

const NODES = { [HUB_CONN.id]: HUB_CONN.cfg };
for (const b of BRANCH_CONNS) NODES[b.id] = b.cfg;

// ---------------------------------------------------------------- table plan
//
// Each entry declares the insert direction(s) per table, in execution order.
// `uniquePrecheck` lists non-PK unique columns that must be collision-checked
// before insert. Adding a table here is the ONLY way it gets reconciled; the
// script refuses to infer direction.

const TABLE_PLAN = {
  customer: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_02"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: ["code"],
    contentAuthority: null,
  },
  repa_deposit: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    contentAuthority: null,
  },
  repa_member: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_02"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    contentAuthority: null,
  },
  repa_member_winner: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    contentAuthority: null,
  },
  sales_order: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    contentAuthority: null,
  },
  sales_order_details: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    contentAuthority: null,
  },
  sales_payment: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    contentAuthority: null,
  },
  deposit: { insert: [], uniquePrecheck: [], contentAuthority: null },
  discount: { insert: [], uniquePrecheck: [], contentAuthority: null },
  stock_requests: { insert: [], uniquePrecheck: [], contentAuthority: null },
  stock_request_items: {
    insert: [],
    uniquePrecheck: [],
    contentAuthority: null,
  },
};

// ---------------------------------------------------------------- CLI args

function parseArgs(argv) {
  const a = {
    apply: false,
    applyContent: false,
    collision: "prefix", // prefix | null | skip  — default per latest instruction
    contentAuthority: {}, // table -> nodeId
    verbose: false,
    tables: null,
    batchSize: 500,
  };
  for (const arg of argv.slice(2)) {
    if (arg === "--apply") a.apply = true;
    else if (arg === "--apply-content") a.applyContent = true;
    else if (arg === "--verbose") a.verbose = true;
    else if (arg.startsWith("--collision=")) a.collision = arg.split("=")[1];
    else if (arg.startsWith("--content-authority=")) {
      const [t, n] = arg.split("=")[1].split(":");
      a.contentAuthority[t] = n;
    } else if (arg.startsWith("--tables=")) {
      a.tables = arg.split("=")[1].split(",").filter(Boolean);
    } else if (arg.startsWith("--batch-size=")) {
      a.batchSize = parseInt(arg.split("=")[1], 10);
    } else if (arg === "--help" || arg === "-h") {
      console.log(
        fs.readFileSync(__filename, "utf8").split("\n").slice(2, 22).join("\n"),
      );
      process.exit(0);
    } else {
      console.error(`Unknown arg: ${arg}`);
      process.exit(2);
    }
  }
  if (!["prefix", "null", "skip"].includes(a.collision)) {
    console.error(`--collision must be prefix|null|skip, got ${a.collision}`);
    process.exit(2);
  }
  return a;
}

const ARGS = parseArgs(process.argv);

// ---------------------------------------------------------------- helpers

function log(...xs) {
  console.log(...xs);
}
function die(msg) {
  console.error("FATAL:", msg);
  process.exit(2);
}

const POOLS = {};
function pool(nodeId) {
  if (!POOLS[nodeId]) {
    const cfg = NODES[nodeId];
    if (!cfg) die(`Unknown node: ${nodeId}`);
    POOLS[nodeId] = mysql.createPool({
      ...cfg,
      waitForConnections: true,
      connectionLimit: 4,
      charset: "utf8mb4",
    });
  }
  return POOLS[nodeId];
}

async function q(nodeId, sql, params) {
  const [rows] = await pool(nodeId).execute(sql, params);
  return rows;
}

async function getPk(table) {
  const rows = await q(
    "NODE_MAIN_HUB",
    `SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
     WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND CONSTRAINT_NAME='PRIMARY'
     ORDER BY ORDINAL_POSITION`,
    [table],
  );
  if (!rows.length) die(`No PRIMARY KEY on ${table}`);
  if (rows.length > 1)
    die(
      `Composite PK on ${table} — fix-drift only supports single-column PKs; extend getPk().`,
    );
  return rows[0].COLUMN_NAME;
}

async function getColumns(table) {
  const rows = await q(
    "NODE_MAIN_HUB",
    `SELECT COLUMN_NAME FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`,
    [table],
  );
  return rows.map((r) => r.COLUMN_NAME);
}

async function getUniqueIndexes(table) {
  const rows = await q(
    "NODE_MAIN_HUB",
    `SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
     FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND Non_unique=0 AND INDEX_NAME<>'PRIMARY'
     ORDER BY INDEX_NAME, SEQ_IN_INDEX`,
    [table],
  );
  const idx = {};
  for (const r of rows) {
    if (!idx[r.INDEX_NAME]) idx[r.INDEX_NAME] = [];
    idx[r.INDEX_NAME].push(r.COLUMN_NAME);
  }
  return idx;
}

// ---------------------------------------------------------------- set reads
//
// For 656k-row tables this materializes the PK set per node; total memory is
// ~40 MB per table per node at 656k rows, acceptable. Batches of 50k rows per
// SELECT to stay under any max_allowed_packet.

async function readPkSet(nodeId, table, pkCol) {
  const set = new Set();
  const CHUNK = 50000;
  let last = null;
  while (true) {
    const sql = last
      ? `SELECT ${pkCol} FROM ${table} WHERE ${pkCol} > ? ORDER BY ${pkCol} LIMIT ${CHUNK}`
      : `SELECT ${pkCol} FROM ${table} ORDER BY ${pkCol} LIMIT ${CHUNK}`;
    const rows = await q(nodeId, sql, last ? [last] : []);
    if (!rows.length) break;
    for (const r of rows) set.add(r[pkCol]);
    last = rows[rows.length - 1][pkCol];
    if (rows.length < CHUNK) break;
  }
  return set;
}

async function readUniqueValueMap(nodeId, table, uniqCols) {
  // Returns Map<compositeKeyString, Set<record_id>> for each unique index that
  // has all its columns defined. NULLs are skipped: MySQL unique indexes allow
  // multiple NULLs, so a NULL never collides.
  if (!uniqCols || !uniqCols.length) return null;
  const cols = uniqCols.join(", ");
  const pk = await getPk(table);
  const rows = await q(nodeId, `SELECT ${pk} AS _pk, ${cols} FROM ${table}`);
  const m = new Map();
  for (const r of rows) {
    const vals = uniqCols.map((c) => r[c]);
    if (vals.some((v) => v === null || v === undefined || v === "")) continue;
    const key = JSON.stringify(vals);
    if (!m.has(key)) m.set(key, new Set());
    m.get(key).add(r._pk);
  }
  return m;
}

// ---------------------------------------------------------------- planning

async function planTable(table, cfg) {
  const pk = await getPk(table);
  const cols = await getColumns(table);
  const uniqIdx = await getUniqueIndexes(table);

  const steps = [];
  // Cache per-node PK sets and unique maps across this table's steps.
  const nodePkSet = {};
  const nodeUniqMap = {};

  async function ensurePkSet(nodeId) {
    if (!nodePkSet[nodeId])
      nodePkSet[nodeId] = await readPkSet(nodeId, table, pk);
    return nodePkSet[nodeId];
  }
  async function ensureUniqMap(nodeId) {
    if (!nodeUniqMap[nodeId]) {
      const pre = cfg.uniquePrecheck || [];
      nodeUniqMap[nodeId] = pre.length
        ? await readUniqueValueMap(nodeId, table, pre)
        : null;
    }
    return nodeUniqMap[nodeId];
  }

  // Content-diff detection runs once per (table, nodeA vs nodeB) pair.
  const contentDiffs = [];

  for (const [srcId, dstId] of cfg.insert) {
    const srcSet = await ensurePkSet(srcId);
    const dstSet = await ensurePkSet(dstId);

    const missing = [];
    for (const id of srcSet) if (!dstSet.has(id)) missing.push(id);

    let toInsert = missing;
    let collisions = [];

    if (cfg.uniquePrecheck && cfg.uniquePrecheck.length && missing.length) {
      const srcUniq = await ensureUniqMap(srcId);
      const dstUniq = await ensureUniqMap(dstId);
      if (srcUniq && dstUniq) {
        // Fetch unique values for the missing rows on src.
        const missingCols = [pk, ...cfg.uniquePrecheck].join(", ");
        // Query in chunks to keep IN() reasonable.
        const CH = 500;
        const rows = [];
        for (let i = 0; i < missing.length; i += CH) {
          const slice = missing.slice(i, i + CH);
          const ph = slice.map(() => "?").join(",");
          const got = await q(
            srcId,
            `SELECT ${missingCols} FROM ${table} WHERE ${pk} IN (${ph})`,
            slice,
          );
          rows.push(...got);
        }

        toInsert = [];
        for (const r of rows) {
          const vals = cfg.uniquePrecheck.map((c) => r[c]);
          if (vals.some((v) => v === null || v === undefined || v === "")) {
            // NULL/empty in a unique index can't collide — safe to insert.
            toInsert.push(r[pk]);
            continue;
          }
          const key = JSON.stringify(vals);
          const dstOwners = dstUniq.get(key);
          const collides =
            dstOwners && dstOwners.size > 0 && !dstOwners.has(r[pk]);
          if (collides) {
            collisions.push({ pk: r[pk], cols: cfg.uniquePrecheck, vals });
          } else {
            toInsert.push(r[pk]);
          }
        }
      }
    }

    steps.push({
      table,
      pk,
      cols,
      from: srcId,
      to: dstId,
      missingCount: missing.length,
      toInsert,
      collisions,
    });
  }

  // Content-diff report — reads both sides, diffs in Node.
  // Only do this for tables explicitly listed in TABLE_PLAN with insert steps OR content authority.
  // We run it for all configured tables with a hub/branch pair when --apply-content or dry-run.
  const allNodes = Object.keys(NODES);
  for (let i = 0; i < allNodes.length; i++) {
    for (let j = i + 1; j < allNodes.length; j++) {
      const nA = allNodes[i],
        nB = allNodes[j];
      // Only diff pairs where at least one direction is configured (avoids 3x work for leaf-leaf pairs).
      const configured = cfg.insert.some(
        ([s, d]) => (s === nA && d === nB) || (s === nB && d === nA),
      );
      if (!configured) continue;
      // Skip tables where content diff detection is pointless (no non-PK columns to differ).
      if (cols.length <= 1) continue;
      const diffs = await diffContent(table, pk, cols, nA, nB);
      if (diffs.length) contentDiffs.push({ nA, nB, diffs });
    }
  }

  return { steps, contentDiffs, pk, cols };
}

async function diffContent(table, pk, cols, nA, nB) {
  const colsList = cols.join(", ");
  // For very large tables, hash-compare in SQL via MD5(CONCAT_WS). We still
  // pull only the diffing PKs. This assumes deterministic collation, which
  // utf8mb4_general_ci is.
  const sql = `
    SELECT a.${pk} AS _pk
    FROM (
      SELECT ${pk}, MD5(CONCAT_WS('#', ${cols.map((c) => `IFNULL(CAST(${c} AS CHAR),'∅')`).join(",")})) AS h
      FROM ${table}
    ) a
    JOIN (
      SELECT ${pk}, MD5(CONCAT_WS('#', ${cols.map((c) => `IFNULL(CAST(${c} AS CHAR),'∅')`).join(",")})) AS h
      FROM ${table}
    ) b ON a.${pk}=b.${pk} AND a.h<>b.h
    LIMIT 1000`;

  // We can't run this cross-node. Fall back to per-node hash dump + local join.
  const HASH_COLS = `${pk}, MD5(CONCAT_WS('#', ${cols.map((c) => `IFNULL(CAST(${c} AS CHAR),'∅')`).join(",")})) AS _h`;
  const [rowA, rowB] = await Promise.all([
    q(nA, `SELECT ${HASH_COLS} FROM ${table}`),
    q(nB, `SELECT ${HASH_COLS} FROM ${table}`),
  ]);
  const mapB = new Map();
  for (const r of rowB) mapB.set(r[pk], r._h);
  const diffPks = [];
  for (const r of rowA) {
    const bH = mapB.get(r[pk]);
    if (bH !== undefined && bH !== r._h) diffPks.push(r[pk]);
  }
  if (!diffPks.length) return [];

  // Fetch full rows for the differing PKs on both sides.
  const CH = 500;
  const rowsA = [],
    rowsB = [];
  for (let i = 0; i < diffPks.length; i += CH) {
    const slice = diffPks.slice(i, i + CH);
    const ph = slice.map(() => "?").join(",");
    const [a, b] = await Promise.all([
      q(nA, `SELECT ${colsList} FROM ${table} WHERE ${pk} IN (${ph})`, slice),
      q(nB, `SELECT ${colsList} FROM ${table} WHERE ${pk} IN (${ph})`, slice),
    ]);
    rowsA.push(...a);
    rowsB.push(...b);
  }
  const byPk = new Map();
  for (const r of rowsA) byPk.set(r[pk], { a: r });
  for (const r of rowsB) {
    const e = byPk.get(r[pk]);
    if (e) e.b = r;
  }
  const out = [];
  for (const [pkVal, { a, b }] of byPk) {
    if (!b) continue;
    const diffCols = [];
    for (const c of cols) {
      if (pkVal && c === pk) continue;
      const av = a[c],
        bv = b[c];
      const sa = av === null ? "∅" : String(av);
      const sb = bv === null ? "∅" : String(bv);
      if (sa !== sb) diffCols.push({ col: c, a: av, b: bv });
    }
    if (diffCols.length) out.push({ pk: pkVal, cols: diffCols });
  }
  return out;
}

// ---------------------------------------------------------------- apply

async function applyInsertStep(step) {
  if (!step.toInsert.length) return { inserted: 0 };
  const cols = step.cols;
  const colList = cols.join(", ");
  const placeholders = `(${cols.map(() => "?").join(",")})`;

  // Fetch full rows for the pk list on the source, in batches matching the INSERT batch size.
  const BATCH = ARGS.batchSize;
  const conn = await pool(step.to).getConnection();
  let inserted = 0;
  try {
    await conn.beginTransaction();
    for (let i = 0; i < step.toInsert.length; i += BATCH) {
      const slice = step.toInsert.slice(i, i + BATCH);
      const ph = slice.map(() => "?").join(",");
      const srcRows = await q(
        step.from,
        `SELECT ${colList} FROM ${step.table} WHERE ${step.pk} IN (${ph})`,
        slice,
      );
      if (!srcRows.length) continue;
      const params = [];
      for (const r of srcRows) for (const c of cols) params.push(r[c]);
      const sql = `INSERT IGNORE INTO ${step.table} (${colList}) VALUES ${srcRows.map(() => placeholders).join(",")}`;
      const [res] = await conn.execute(sql, params);
      inserted += res.affectedRows;
    }
    await conn.commit();
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
  return { inserted };
}

async function applyCodeCollisionRenames(table, collisions, pk, mode) {
  if (!collisions.length) return { renamed: 0 };
  if (mode === "skip") return { renamed: 0, skipped: collisions.length };
  // We rename on the *source* branch, so the insert step can proceed.
  // Get the branch that had the collisions from the step context; callers pass it via argv? No —
  // we track per (table, from) below; this function is called from the run loop with explicit nodeId.
  // See runTable().
  return { renamed: 0 }; // filled by runTable
}

async function renameCollidingCodes(nodeId, table, pkCol, collisions, mode) {
  const conn = await pool(nodeId).getConnection();
  try {
    await conn.beginTransaction();
    let n = 0;
    for (const c of collisions) {
      if (mode === "null") {
        const [r] = await conn.execute(
          `UPDATE ${table} SET ${c.cols[0]}=NULL WHERE ${pkCol}=?`,
          [c.pk],
        );
        n += r.affectedRows;
      } else if (mode === "prefix") {
        const suffix =
          "-" +
          nodeId
            .replace("NODE_", "")
            .replace("_MAIN_HUB", "HUB")
            .replace("_BRANCH_", "BR");
        const [r] = await conn.execute(
          `UPDATE ${table} SET ${c.cols[0]}=CONCAT(${c.cols[0]}, ?) WHERE ${pkCol}=?`,
          [suffix, c.pk],
        );
        n += r.affectedRows;
      }
    }
    await conn.commit();
    return { renamed: n };
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
}

async function applyContentDiffs(contentDiffs, cfg) {
  // Never auto-applies unless (a) --apply-content, (b) authority node configured
  // for this table. Authority node's row wins; UPDATEs are pushed to the other node(s).
  const applied = [];
  for (const { nA, nB, diffs } of contentDiffs) {
    for (const d of diffs) {
      const winner = cfg.contentAuthority;
      if (!winner || (winner !== nA && winner !== nB)) continue;
      const loser = winner === nA ? nB : nA;
      const winnerRow =
        winner === nA
          ? d.cols.map((c) => [c.col, c.a])
          : d.cols.map((c) => [c.col, c.b]);
      const setSql = winnerRow.map(([c]) => `${c}=?`).join(", ");
      const params = winnerRow.map(([, v]) => v);
      params.push(d.pk);
      const [res] = await pool(loser).execute(
        `UPDATE ${cfg.__table} SET ${setSql} WHERE ${cfg.__pk}=?`,
        params,
      );
      applied.push({
        table: cfg.__table,
        pk: d.pk,
        loser,
        affected: res.affectedRows,
      });
    }
  }
  return applied;
}

// ---------------------------------------------------------------- gates

function gateBeforeApply(plan) {
  const errors = [];

  // 1. pm2 engine online
  try {
    const out = execSync("pm2 jlist 2>/dev/null", { encoding: "utf8" });
    const arr = JSON.parse(out);
    const eng = arr.find((p) => p.name === "replica-engine");
    if (!eng) errors.push('pm2 process "replica-engine" not found');
    else if (eng.pm2_env.status !== "online")
      errors.push(`replica-engine status is ${eng.pm2_env.status}`);
    else if (eng.pm2_env.restart_time > 0) {
      // We allow restarts but warn; only block if uptime < 10 min.
      const up = (Date.now() - eng.pm2_env.pm_uptime) / 60000;
      if (up < 10)
        errors.push(
          `replica-engine uptime ${up.toFixed(1)}m — wait for stability`,
        );
    }
  } catch (e) {
    errors.push(`pm2 check failed: ${e.message}`);
  }

  // 2. Backups fresh — look for backups/ dir with files < 1h old for each plan table
  const bdir = path.join(__dirname, "backups");
  if (!fs.existsSync(bdir)) {
    errors.push(`backups dir missing: ${bdir}`);
  } else {
    const files = fs.readdirSync(bdir).filter((f) => f.endsWith(".sql"));
    const oneHourAgo = Date.now() - 3600_000;
    const fresh = files.filter(
      (f) => fs.statSync(path.join(bdir, f)).mtimeMs > oneHourAgo,
    );
    const tables = Object.keys(plan);
    for (const t of tables) {
      if (!fresh.some((f) => f.includes(t))) {
        errors.push(`no backup <1h old for table ${t} in ${bdir}`);
      }
    }
  }

  return errors;
}

// ---------------------------------------------------------------- verify

function postVerify() {
  try {
    const out = execSync("node node-copy.js --verify 2>&1", {
      encoding: "utf8",
      timeout: 600_000,
    });
    const resultLine = out.split("\n").find((l) => l.includes("Result:")) || "";
    const clean = resultLine.includes("all tables in parity");
    const warnings = out.split("\n").filter((l) => l.includes("WARN"));
    return { clean, resultLine: resultLine.trim(), warnings };
  } catch (e) {
    return {
      clean: false,
      resultLine: `verify failed: ${e.message}`,
      warnings: [],
    };
  }
}

// ---------------------------------------------------------------- main

async function main() {
  log(
    `fix-drift.js — mode=${ARGS.apply ? "APPLY" : "DRY-RUN"} collision=${ARGS.collision}`,
  );

  const tableNames = ARGS.tables || Object.keys(TABLE_PLAN);
  const plan = {};

  for (const table of tableNames) {
    const cfg = TABLE_PLAN[table];
    if (!cfg) {
      log(`skip ${table}: not in TABLE_PLAN`);
      continue;
    }
    log(`\n=== ${table} ===`);
    try {
      const p = await planTable(table, cfg);
      plan[table] = p;
      for (const s of p.steps) {
        log(
          `  ${s.from} -> ${s.to}: missing=${s.missingCount} insert=${s.toInsert.length} collisions=${s.collisions.length}`,
        );
        if (
          s.collisions.length &&
          (ARGS.verbose || s.collisions.length <= 20)
        ) {
          for (const c of s.collisions)
            log(
              `    COLLISION ${c.pk} ${c.cols.join(",")}=${JSON.stringify(c.vals)}`,
            );
        } else if (s.collisions.length) {
          log(
            `    (${s.collisions.length} collisions; --verbose for full list)`,
          );
        }
      }
      for (const { nA, nB, diffs } of p.contentDiffs) {
        log(`  content diffs ${nA} vs ${nB}: ${diffs.length}`);
        for (const d of diffs.slice(0, ARGS.verbose ? diffs.length : 5)) {
          log(
            `    DIFF ${d.pk}: ${d.cols.map((c) => `${c.col}:${JSON.stringify(c.a)}→${JSON.stringify(c.b)}`).join(" ")}`,
          );
        }
        if (diffs.length > 5 && !ARGS.verbose)
          log(`    (${diffs.length - 5} more; --verbose to show all)`);
      }
    } catch (e) {
      log(`  ERROR planning ${table}: ${e.message}`);
    }
  }

  if (!ARGS.apply) {
    log("\n(dry-run — pass --apply to execute row-set inserts)");
    process.exit(1);
  }

  // Gates
  const gateErrors = gateBeforeApply(plan);
  if (gateErrors.length) {
    for (const e of gateErrors) console.error("GATE:", e);
    die("pre-apply gates failed");
  }

  // Execute row-set inserts in table plan order.
  for (const table of Object.keys(plan)) {
    const cfg = TABLE_PLAN[table];
    const p = plan[table];
    if (!p) continue;
    log(`\n--- apply ${table} ---`);

    // 1. Resolve code collisions on the source if any.
    for (const s of p.steps) {
      if (s.collisions.length) {
        if (ARGS.collision === "skip") {
          log(
            `  ${s.from}->${s.to}: skipping ${s.collisions.length} colliding rows (--collision=skip)`,
          );
          continue;
        }
        log(
          `  ${s.from}->${s.to}: renaming ${s.collisions.length} colliding codes on ${s.from} (--collision=${ARGS.collision})`,
        );
        const r = await renameCollidingCodes(
          s.from,
          table,
          p.pk,
          s.collisions,
          ARGS.collision,
        );
        log(`    renamed/updated: ${r.renamed}`);
        // Re-plan just this step's insert set: fetch the affected rows again.
        const slice = s.collisions.map((c) => c.pk);
        const CH = 500;
        const colsList = p.cols.join(", ");
        for (let i = 0; i < slice.length; i += CH) {
          const ph = slice
            .slice(i, i + CH)
            .map(() => "?")
            .join(",");
          const got = await q(
            s.from,
            `SELECT ${p.pk} FROM ${table} WHERE ${p.pk} IN (${ph})`,
            slice.slice(i, i + CH),
          );
          for (const r2 of got) s.toInsert.push(r2[p.pk]);
        }
      }
    }

    // 2. Insert each step (order preserved from TABLE_PLAN).
    for (const s of p.steps) {
      if (!s.toInsert.length) {
        log(`  ${s.from}->${s.to}: nothing to insert`);
        continue;
      }
      try {
        const r = await applyInsertStep(s);
        log(
          `  ${s.from}->${s.to}: inserted ${r.inserted}/${s.toInsert.length}`,
        );
      } catch (e) {
        console.error(
          `  ERROR inserting ${s.from}->${s.to} on ${table}:`,
          e.message,
        );
      }
    }

    // 3. Content diffs (never auto-applied unless --apply-content + authority).
    if (p.contentDiffs.length && ARGS.applyContent) {
      const cfgWithIds = { ...cfg, __table: table, __pk: p.pk };
      const applied = await applyContentDiffs(p.contentDiffs, cfgWithIds);
      if (applied.length) log(`  content diffs applied: ${applied.length}`);
      else
        log(
          `  content diffs present but no authority configured — not applied`,
        );
    } else if (p.contentDiffs.length) {
      log(
        `  content diffs present (${p.contentDiffs.reduce((n, c) => n + c.diffs.length, 0)}) — report-only`,
      );
    }
  }

  // Post-verify
  log("\n--- post-apply verify ---");
  const v = postVerify();
  log(v.resultLine);
  if (v.warnings.length) {
    log(`${v.warnings.length} WARN lines:`);
    for (const w of v.warnings.slice(0, 20)) log(`  ${w}`);
    if (v.warnings.length > 20) log(`  ... +${v.warnings.length - 20} more`);
  }
  process.exit(v.clean ? 0 : 3);
}

main().catch((e) => {
  console.error(e);
  process.exit(2);
});
