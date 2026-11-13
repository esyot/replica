#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const { execSync } = require("child_process");
const mysql = require("mysql2/promise");

// ---------------------------------------------------------------- env

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
    )
      v = v.slice(1, -1);
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

const TABLE_PLAN = {
  customer: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_02"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: ["code"],
    identityCols: ["name"],
    contentDiffIgnoreCols: ["address"],
    contentAuthority: null,
    safeToAutoApply: false, // blocked on BR02 corruption root-cause
  },
  repa_deposit: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    identityCols: [],
    contentDiffIgnoreCols: [],
    contentAuthority: null,
    safeToAutoApply: true,
  },
  repa_member: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_02"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    identityCols: [],
    contentDiffIgnoreCols: [],
    contentAuthority: null,
    safeToAutoApply: true,
  },
  repa_member_winner: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    identityCols: [],
    contentDiffIgnoreCols: [],
    contentAuthority: null,
    safeToAutoApply: true,
  },
  sales_order: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    identityCols: ["customer"],
    contentDiffIgnoreCols: ["customer", "date_ordered"],
    contentAuthority: null,
    safeToAutoApply: false, // blocked on BR02 corruption root-cause
  },
  sales_order_details: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    identityCols: [],
    contentDiffIgnoreCols: [],
    contentAuthority: null,
    safeToAutoApply: false, // FK on sales_order
  },
  sales_payment: {
    insert: [
      ["NODE_BRANCH_02", "NODE_MAIN_HUB"],
      ["NODE_MAIN_HUB", "NODE_BRANCH_03"],
    ],
    uniquePrecheck: [],
    identityCols: [],
    contentDiffIgnoreCols: [],
    contentAuthority: null,
    safeToAutoApply: false, // FK on sales_order
  },
  deposit: {
    insert: [],
    uniquePrecheck: [],
    identityCols: [],
    contentDiffIgnoreCols: [],
    contentAuthority: null,
    safeToAutoApply: true,
  },
  discount: {
    insert: [],
    uniquePrecheck: [],
    identityCols: [],
    contentDiffIgnoreCols: [],
    contentAuthority: null,
    safeToAutoApply: true,
  },
  stock_requests: {
    insert: [],
    uniquePrecheck: [],
    identityCols: [],
    contentDiffIgnoreCols: [],
    contentAuthority: null,
    safeToAutoApply: true,
  },
  stock_request_items: {
    insert: [],
    uniquePrecheck: [],
    identityCols: [],
    contentDiffIgnoreCols: [],
    contentAuthority: null,
    safeToAutoApply: true,
  },
};

// ---------------------------------------------------------------- CLI

function parseArgs(argv) {
  const a = {
    apply: false,
    applyContent: false,
    onlySafe: false,
    collision: "prefix",
    contentAuthority: {},
    verbose: false,
    tables: null,
    batchSize: 500,
    forceBulk: false,
  };
  for (const arg of argv.slice(2)) {
    if (arg === "--apply") a.apply = true;
    else if (arg === "--apply-content") a.applyContent = true;
    else if (arg === "--only-safe") a.onlySafe = true;
    else if (arg === "--verbose") a.verbose = true;
    else if (arg === "--force-bulk") a.forceBulk = true;
    else if (arg.startsWith("--collision=")) a.collision = arg.split("=")[1];
    else if (arg.startsWith("--content-authority=")) {
      const [t, n] = arg.split("=")[1].split(":");
      a.contentAuthority[t] = n;
    } else if (arg.startsWith("--tables="))
      a.tables = arg.split("=")[1].split(",").filter(Boolean);
    else if (arg.startsWith("--batch-size="))
      a.batchSize = parseInt(arg.split("=")[1], 10);
    else if (arg === "--help" || arg === "-h") {
      console.log(
        fs.readFileSync(__filename, "utf8").split("\n").slice(2, 30).join("\n"),
      );
      process.exit(0);
    } else {
      console.error(`Unknown arg: ${arg}`);
      process.exit(2);
    }
  }
  if (!["prefix", "null", "skip"].includes(a.collision)) {
    console.error(`--collision must be prefix|null|skip`);
    process.exit(2);
  }
  return a;
}

const ARGS = parseArgs(process.argv);

function log(...xs) {
  console.log(...xs);
}
function die(msg) {
  console.error("FATAL:", msg);
  process.exit(2);
}

// ---------------------------------------------------------------- db

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

const bt = (c) => `\`${c}\``; // backtick an identifier
const btAll = (cs) => cs.map(bt).join(", ");

async function getPk(table) {
  const rows = await q(
    "NODE_MAIN_HUB",
    `SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
     WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND CONSTRAINT_NAME='PRIMARY'
     ORDER BY ORDINAL_POSITION`,
    [table],
  );
  if (!rows.length) die(`No PRIMARY KEY on ${table}`);
  if (rows.length > 1) die(`Composite PK on ${table} — extend getPk().`);
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

// ---------------------------------------------------------------- set reads

async function readPkSet(nodeId, table, pkCol) {
  const set = new Set();
  const CHUNK = 50000;
  let last = null;
  while (true) {
    const sql = last
      ? `SELECT ${bt(pkCol)} FROM ${bt(table)} WHERE ${bt(pkCol)} > ? ORDER BY ${bt(pkCol)} LIMIT ${CHUNK}`
      : `SELECT ${bt(pkCol)} FROM ${bt(table)} ORDER BY ${bt(pkCol)} LIMIT ${CHUNK}`;
    const rows = await q(nodeId, sql, last ? [last] : []);
    if (!rows.length) break;
    for (const r of rows) set.add(r[pkCol]);
    last = rows[rows.length - 1][pkCol];
    if (rows.length < CHUNK) break;
  }
  return set;
}

async function readUniqueValueMap(nodeId, table, uniqCols) {
  if (!uniqCols || !uniqCols.length) return null;
  const pk = await getPk(table);
  const rows = await q(
    nodeId,
    `SELECT ${bt(pk)} AS _pk, ${btAll(uniqCols)} FROM ${bt(table)}`,
  );
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
  const steps = [];
  const nodePkSet = {};
  const nodeUniqMap = {};

  async function ensurePkSet(n) {
    if (!nodePkSet[n]) nodePkSet[n] = await readPkSet(n, table, pk);
    return nodePkSet[n];
  }
  async function ensureUniqMap(n) {
    if (!nodeUniqMap[n]) {
      const pre = cfg.uniquePrecheck || [];
      nodeUniqMap[n] = pre.length
        ? await readUniqueValueMap(n, table, pre)
        : null;
    }
    return nodeUniqMap[n];
  }

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
        const missingCols = btAll([pk, ...cfg.uniquePrecheck]);
        const CH = 500;
        const rows = [];
        for (let i = 0; i < missing.length; i += CH) {
          const slice = missing.slice(i, i + CH);
          const ph = slice.map(() => "?").join(",");
          const got = await q(
            srcId,
            `SELECT ${missingCols} FROM ${bt(table)} WHERE ${bt(pk)} IN (${ph})`,
            slice,
          );
          rows.push(...got);
        }
        toInsert = [];
        for (const r of rows) {
          const vals = cfg.uniquePrecheck.map((c) => r[c]);
          if (vals.some((v) => v === null || v === undefined || v === "")) {
            toInsert.push(r[pk]);
            continue;
          }
          const key = JSON.stringify(vals);
          const owners = dstUniq.get(key);
          const collides = owners && owners.size > 0 && !owners.has(r[pk]);
          if (collides)
            collisions.push({ pk: r[pk], cols: cfg.uniquePrecheck, vals });
          else toInsert.push(r[pk]);
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

  const allNodes = Object.keys(NODES);
  for (let i = 0; i < allNodes.length; i++) {
    for (let j = i + 1; j < allNodes.length; j++) {
      const nA = allNodes[i],
        nB = allNodes[j];
      const configured = cfg.insert.some(
        ([s, d]) => (s === nA && d === nB) || (s === nB && d === nA),
      );
      if (!configured) continue;
      if (cols.length <= 1) continue;
      const diffs = await diffContent(table, pk, cols, nA, nB);
      if (diffs.length)
        contentDiffs.push({
          nA,
          nB,
          diffs,
          guard: corruptionGuard(cfg, diffs, nA, nB),
        });
    }
  }

  return { steps, contentDiffs, pk, cols };
}

async function diffContent(table, pk, cols, nA, nB) {
  const hashExpr = cols
    .map((c) => `IFNULL(CAST(${bt(c)} AS CHAR),'\u2205')`)
    .join(",");
  const HASH_COLS = `${bt(pk)}, MD5(CONCAT_WS('#', ${hashExpr})) AS _h`;
  const [rowA, rowB] = await Promise.all([
    q(nA, `SELECT ${HASH_COLS} FROM ${bt(table)}`),
    q(nB, `SELECT ${HASH_COLS} FROM ${bt(table)}`),
  ]);
  const mapB = new Map();
  for (const r of rowB) mapB.set(r[pk], r._h);
  const diffPks = [];
  for (const r of rowA) {
    const bH = mapB.get(r[pk]);
    if (bH !== undefined && bH !== r._h) diffPks.push(r[pk]);
  }
  if (!diffPks.length) return [];

  const CH = 500;
  const rowsA = [],
    rowsB = [];
  const colsList = btAll(cols);
  for (let i = 0; i < diffPks.length; i += CH) {
    const slice = diffPks.slice(i, i + CH);
    const ph = slice.map(() => "?").join(",");
    const [a, b] = await Promise.all([
      q(
        nA,
        `SELECT ${colsList} FROM ${bt(table)} WHERE ${bt(pk)} IN (${ph})`,
        slice,
      ),
      q(
        nB,
        `SELECT ${colsList} FROM ${bt(table)} WHERE ${bt(pk)} IN (${ph})`,
        slice,
      ),
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
      if (c === pk) continue;
      const av = a[c],
        bv = b[c];
      const sa = av === null ? "\u2205" : String(av);
      const sb = bv === null ? "\u2205" : String(bv);
      if (sa !== sb) diffCols.push({ col: c, a: av, b: bv });
    }
    if (diffCols.length) out.push({ pk: pkVal, cols: diffCols, a, b });
  }
  return out;
}

// ---------------------------------------------------------------- corruption guard
//
// Classifies content diffs so --apply-content can refuse unsafe tables.
//   CLEAN            — no diffs, or none matching bulk/identity patterns
//   BULK_TIMESTAMP   — >=50% of diffs have one side's updated_at equal to a
//                      single value that is absent on the other side; the table
//                      was bulk-rewritten at one instant (corruption signature)
//   PK_REUSE         — at least one diff has different identityCols on the two
//                      sides (same record_id, different person/entity)
//   MIXED            — both of the above

function corruptionGuard(cfg, diffs, nA, nB) {
  if (!diffs.length) return { kind: "CLEAN" };

  // PK reuse: any diff where identityCols differ
  const idCols = cfg.identityCols || [];
  let pkReuse = null;
  if (idCols.length) {
    for (const d of diffs) {
      const idDiff = d.cols.find((c) => idCols.includes(c.col));
      if (idDiff) {
        pkReuse = { pk: d.pk, col: idDiff.col, a: idDiff.a, b: idDiff.b };
        break;
      }
    }
  }

  // Bulk timestamp: count diffs where a side's updated_at is identical across rows
  let bulk = null;
  const updDiff = diffs
    .map((d) => d.cols.find((c) => c.col === "updated_at"))
    .filter(Boolean);
  if (updDiff.length >= Math.ceil(diffs.length * 0.5)) {
    const aVals = updDiff.map((c) => (c.a === null ? null : String(c.a)));
    const bVals = updDiff.map((c) => (c.b === null ? null : String(c.b)));
    const tally = (vals) => {
      const m = new Map();
      for (const v of vals) if (v !== null) m.set(v, (m.get(v) || 0) + 1);
      let top = null,
        n = 0;
      for (const [k, v] of m)
        if (v > n) {
          top = k;
          n = v;
        }
      return { top, n, total: vals.length };
    };
    const tA = tally(aVals);
    const tB = tally(bVals);
    if (tA.top && tA.n >= Math.ceil(aVals.length * 0.5))
      bulk = { side: nA, value: tA.top, n: tA.n, total: aVals.length };
    else if (tB.top && tB.n >= Math.ceil(bVals.length * 0.5))
      bulk = { side: nB, value: tB.top, n: tB.n, total: bVals.length };
  }

  if (pkReuse && bulk) return { kind: "MIXED", pkReuse, bulk };
  if (pkReuse) return { kind: "PK_REUSE", pkReuse };
  if (bulk) return { kind: "BULK_TIMESTAMP", bulk };
  return { kind: "CLEAN" };
}

// ---------------------------------------------------------------- apply

async function applyInsertStep(step) {
  if (!step.toInsert.length) return { inserted: 0 };
  const colList = btAll(step.cols);
  const placeholders = `(${step.cols.map(() => "?").join(",")})`;
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
        `SELECT ${colList} FROM ${bt(step.table)} WHERE ${bt(step.pk)} IN (${ph})`,
        slice,
      );
      if (!srcRows.length) continue;
      const params = [];
      for (const r of srcRows) for (const c of step.cols) params.push(r[c]);
      const sql = `INSERT IGNORE INTO ${bt(step.table)} (${colList}) VALUES ${srcRows.map(() => placeholders).join(",")}`;
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

async function renameCollidingCodes(nodeId, table, pkCol, collisions, mode) {
  if (!collisions.length || mode === "skip") return { renamed: 0 };
  const conn = await pool(nodeId).getConnection();
  try {
    await conn.beginTransaction();
    let n = 0;
    for (const c of collisions) {
      if (mode === "null") {
        const [r] = await conn.execute(
          `UPDATE ${bt(table)} SET ${bt(c.cols[0])}=NULL WHERE ${bt(pkCol)}=?`,
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
          `UPDATE ${bt(table)} SET ${bt(c.cols[0])}=CONCAT(${bt(c.cols[0])}, ?) WHERE ${bt(pkCol)}=?`,
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
  const ignore = new Set(cfg.contentDiffIgnoreCols || []);
  const applied = [];
  for (const { nA, nB, diffs, guard } of contentDiffs) {
    if (guard.kind === "PK_REUSE" || guard.kind === "MIXED") continue;
    if (guard.kind === "BULK_TIMESTAMP" && !ARGS.forceBulk) continue;

    for (const d of diffs) {
      const winner = cfg.contentAuthority;
      if (!winner || (winner !== nA && winner !== nB)) continue;
      const loser = winner === nA ? nB : nA;
      const winnerRow = (
        winner === nA
          ? d.cols.map((c) => [c.col, c.a])
          : d.cols.map((c) => [c.col, c.b])
      ).filter(([c]) => !ignore.has(c));
      if (!winnerRow.length) continue;
      const setSql = winnerRow.map(([c]) => `${bt(c)}=?`).join(", ");
      const params = winnerRow.map(([, v]) => v);
      params.push(d.pk);
      const [res] = await pool(loser).execute(
        `UPDATE ${bt(cfg.__table)} SET ${setSql} WHERE ${bt(cfg.__pk)}=?`,
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
  try {
    const out = execSync("pm2 jlist 2>/dev/null", { encoding: "utf8" });
    const arr = JSON.parse(out);
    const eng = arr.find((p) => p.name === "replica-engine");
    if (!eng) errors.push('pm2 process "replica-engine" not found');
    else if (eng.pm2_env.status !== "online")
      errors.push(`replica-engine status is ${eng.pm2_env.status}`);
    else if (eng.pm2_env.restart_time > 0) {
      const up = (Date.now() - eng.pm2_env.pm_uptime) / 60000;
      if (up < 10)
        errors.push(
          `replica-engine uptime ${up.toFixed(1)}m — wait for stability`,
        );
    }
  } catch (e) {
    errors.push(`pm2 check failed: ${e.message}`);
  }

  const bdir = path.join(__dirname, "backups");
  if (!fs.existsSync(bdir)) errors.push(`backups dir missing: ${bdir}`);
  else {
    const files = fs.readdirSync(bdir).filter((f) => f.endsWith(".sql"));
    const oneHourAgo = Date.now() - 3600_000;
    const fresh = files.filter(
      (f) => fs.statSync(path.join(bdir, f)).mtimeMs > oneHourAgo,
    );
    for (const t of Object.keys(plan)) {
      if (!fresh.some((f) => f.includes(t)))
        errors.push(`no backup <1h old for ${t} in ${bdir}`);
    }
  }
  return errors;
}

function postVerify() {
  try {
    const out = execSync("node node-copy.js --verify 2>&1", {
      encoding: "utf8",
      timeout: 600_000,
    });
    const resultLine = (
      out.split("\n").find((l) => l.includes("Result:")) || ""
    ).trim();
    return {
      clean: resultLine.includes("all tables in parity"),
      resultLine,
      warnings: out.split("\n").filter((l) => l.includes("WARN")),
    };
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
    `fix-drift.js — mode=${ARGS.apply ? "APPLY" : "DRY-RUN"} collision=${ARGS.collision} onlySafe=${ARGS.onlySafe}`,
  );

  let tableNames = ARGS.tables || Object.keys(TABLE_PLAN);
  if (ARGS.onlySafe)
    tableNames = tableNames.filter(
      (t) => TABLE_PLAN[t] && TABLE_PLAN[t].safeToAutoApply,
    );

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
        } else if (s.collisions.length)
          log(
            `    (${s.collisions.length} collisions; --verbose for full list)`,
          );
      }
      for (const { nA, nB, diffs, guard } of p.contentDiffs) {
        log(
          `  content diffs ${nA} vs ${nB}: ${diffs.length}  guard=${guard.kind}`,
        );
        if (guard.kind === "BULK_TIMESTAMP" || guard.kind === "MIXED") {
          const b = guard.bulk;
          log(
            `    BULK_TIMESTAMP: ${b.n}/${b.total} diffs share updated_at=${b.value} on ${b.side}`,
          );
        }
        if (guard.kind === "PK_REUSE" || guard.kind === "MIXED") {
          const r = guard.pkReuse;
          log(
            `    PK_REUSE sample: ${r.pk} ${r.col}: ${JSON.stringify(r.a)} vs ${JSON.stringify(r.b)}`,
          );
        }
        if (guard.kind === "CLEAN") {
          for (const d of diffs.slice(0, ARGS.verbose ? diffs.length : 5)) {
            log(
              `    DIFF ${d.pk}: ${d.cols.map((c) => `${c.col}:${JSON.stringify(c.a)}\u2192${JSON.stringify(c.b)}`).join(" ")}`,
            );
          }
          if (diffs.length > 5 && !ARGS.verbose)
            log(`    (${diffs.length - 5} more; --verbose to show all)`);
        }
      }
    } catch (e) {
      log(`  ERROR planning ${table}: ${e.message}`);
    }
  }

  if (!ARGS.apply) {
    log("\n(dry-run — pass --apply to execute row-set inserts)");
    process.exit(1);
  }

  const gateErrors = gateBeforeApply(plan);
  if (gateErrors.length) {
    for (const e of gateErrors) console.error("GATE:", e);
    die("pre-apply gates failed");
  }

  for (const table of Object.keys(plan)) {
    const cfg = TABLE_PLAN[table];
    const p = plan[table];
    if (!p) continue;
    if (ARGS.onlySafe && !cfg.safeToAutoApply) {
      log(`\n--- skip ${table}: not safeToAutoApply ---`);
      continue;
    }
    log(`\n--- apply ${table} ---`);

    for (const s of p.steps) {
      if (!s.collisions.length) continue;
      if (ARGS.collision === "skip") {
        log(`  ${s.from}->${s.to}: skipping ${s.collisions.length} collisions`);
        continue;
      }
      log(
        `  ${s.from}->${s.to}: renaming ${s.collisions.length} colliding codes on ${s.from} (${ARGS.collision})`,
      );
      const r = await renameCollidingCodes(
        s.from,
        table,
        p.pk,
        s.collisions,
        ARGS.collision,
      );
      log(`    renamed: ${r.renamed}`);
      const slice = s.collisions.map((c) => c.pk);
      const CH = 500;
      for (let i = 0; i < slice.length; i += CH) {
        const ph = slice
          .slice(i, i + CH)
          .map(() => "?")
          .join(",");
        const got = await q(
          s.from,
          `SELECT ${bt(p.pk)} FROM ${bt(table)} WHERE ${bt(p.pk)} IN (${ph})`,
          slice.slice(i, i + CH),
        );
        for (const r2 of got) s.toInsert.push(r2[p.pk]);
      }
    }

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

    if (p.contentDiffs.length && ARGS.applyContent) {
      const cfgWithIds = { ...cfg, __table: table, __pk: p.pk };
      const applied = await applyContentDiffs(p.contentDiffs, cfgWithIds);
      const blocked = p.contentDiffs.filter(
        (c) => c.guard.kind !== "CLEAN" && c.guard.kind !== "BULK_TIMESTAMP",
      );
      if (applied.length) log(`  content diffs applied: ${applied.length}`);
      else
        log(
          `  content diffs present but no authority configured or guarded — not applied`,
        );
      if (blocked.length)
        log(
          `  ${blocked.length} node-pair(s) refused due to ${blocked.map((b) => b.guard.kind).join(",")}`,
        );
    } else if (p.contentDiffs.length) {
      log(
        `  content diffs present (${p.contentDiffs.reduce((n, c) => n + c.diffs.length, 0)}) — report-only`,
      );
    }
  }

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
