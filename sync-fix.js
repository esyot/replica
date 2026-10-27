#!/usr/bin/env node
/**
 * sync-fix.js — one-shot fixer. Analyzes, backfills, cleans, verifies.
 *
 * DEFAULT IS DRY-RUN. Use --apply to actually write.
 *
 * Usage:
 *   node sync-fix.js                       # DRY RUN — all branches, shows what needs fixing
 *   node sync-fix.js --apply               # actually fix all branches
 *   node sync-fix.js NODE_BRANCH_03        # DRY RUN — only branch 03
 *   node sync-fix.js --apply NODE_BRANCH_03
 *   node sync-fix.js --list                # list configured branches and exit
 *
 * Env: same as server.js (LOCAL_*, BRANCHES_JSON, ALLOWED_TABLES, etc.)
 *
 * Exit codes:
 *   0 = all in parity (or dry-run: no gaps found)
 *   1 = some gaps remain (dry-run: gaps exist but nothing written)
 *   2 = fatal error
 */

require("dotenv").config();
const knex = require("knex");
const winston = require("winston");

const { getTablePkColumns, ALLOWED_TABLES } = require("./triggers");

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
/* CLI                                                                */
/* ------------------------------------------------------------------ */

const argv = process.argv.slice(2);
const APPLY = argv.includes("--apply");
const LIST = argv.includes("--list");
const branchArg = argv.find((a) => !a.startsWith("-"));

if (!APPLY && !LIST) {
  logger.warn(
    "==================================================================\n" +
      "  DRY RUN MODE — no writes will happen.\n" +
      "  Review the output below, then re-run with --apply to execute.\n" +
      "==================================================================",
  );
} else if (APPLY) {
  logger.warn(
    "==================================================================\n" +
      "  APPLY MODE — writing to databases.\n" +
      "  Make sure server.js is STOPPED on all nodes (Ctrl+C within 5s).\n" +
      "==================================================================",
  );
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
/* Connection helpers                                                 */
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
function parseConn(url, dp = 3306) {
  const p = new URL(url);
  return {
    host: p.hostname || "127.0.0.1",
    port: p.port ? parseInt(p.port, 10) : dp,
    user: p.username ? decodeURIComponent(p.username) : undefined,
    password: p.password ? decodeURIComponent(p.password) : undefined,
    database: p.pathname.slice(1),
  };
}
function buildKnex(url, client, ssl, sslStrict) {
  if (isPg(client)) {
    return knex({
      client,
      connection: {
        connectionString: url,
        ssl: ssl ? { rejectUnauthorized: sslStrict !== false } : false,
      },
      pool: { min: 0, max: 4 },
    });
  }
  return knex({ client, connection: parseConn(url), pool: { min: 0, max: 4 } });
}

function buildHub() {
  const client = normalizeClient(process.env.LOCAL_CLIENT);
  return {
    id: process.env.LOCAL_ID || "NODE_MAIN_HUB",
    client,
    db: buildKnex(
      process.env.LOCAL_URL,
      client,
      process.env.LOCAL_SSL === "true",
      process.env.LOCAL_SSL_STRICT !== "false",
    ),
  };
}

function buildBranches(filterId) {
  const cfg = JSON.parse(process.env.BRANCHES_JSON || "[]");
  const list = filterId ? cfg.filter((b) => b.id === filterId) : cfg;
  if (filterId && list.length === 0) {
    logger.error(`Unknown branch: ${filterId}`);
    logger.info(`Available branches: ${cfg.map((b) => b.id).join(", ")}`);
    process.exit(2);
  }
  return list.map((b) => {
    const client = normalizeClient(b.client);
    return {
      id: b.id,
      client,
      db: buildKnex(b.url, client, true, b.sslInsecure !== true),
    };
  });
}

/* ------------------------------------------------------------------ */
/* PK helpers                                                         */
/* ------------------------------------------------------------------ */

const PK_CACHE = new Map();
async function pkCols(node, table) {
  const key = `${node.id}:${table}`;
  if (!PK_CACHE.has(key)) {
    const p = getTablePkColumns(node, table).then((c) => {
      if (!c || !c.length) throw new Error(`no PK for ${table}`);
      return c;
    });
    p.catch(() => PK_CACHE.delete(key));
    PK_CACHE.set(key, p);
  }
  return PK_CACHE.get(key);
}
function pkKey(row, cols) {
  if (cols.length === 1)
    return row[cols[0]] == null ? null : String(row[cols[0]]);
  return JSON.stringify(cols.map((c) => row[c] ?? null));
}

/* ------------------------------------------------------------------ */
/* Diff: what's missing                                             */
/* ------------------------------------------------------------------ */

async function findMissing(source, target, table) {
  const srcCols = await pkCols(source, table);
  const tgtCols = await pkCols(target, table);
  const [srcRows, tgtRows] = await Promise.all([
    source.db(table).select(srcCols),
    target.db(table).select(tgtCols),
  ]);
  const tgtSet = new Set();
  for (const r of tgtRows) {
    const k = pkKey(r, tgtCols);
    if (k) tgtSet.add(k);
  }
  const missing = [];
  for (const r of srcRows) {
    const k = pkKey(r, srcCols);
    if (k && !tgtSet.has(k)) missing.push(k);
  }
  return { missing, srcCols, tgtCols };
}

/* ------------------------------------------------------------------ */
/* Copy one table (chunked, per-row fallback, deferred on FK)         */
/* ------------------------------------------------------------------ */

async function copyTable(source, target, table, keys, pkColumns) {
  const deferred = [];
  const errors = [];
  let copied = 0;

  const FK_RE = /foreign key|ER_NO_REFERENCED_ROW|ER_ROW_IS_REFERENCED/i;

  const fetchChunk = 200;
  const allRows = [];
  for (let i = 0; i < keys.length; i += fetchChunk) {
    const chunk = keys.slice(i, i + fetchChunk);
    let rows;
    if (pkColumns.length === 1) {
      rows = await source.db(table).whereIn(pkColumns[0], chunk);
    } else {
      const b = source.db(table);
      b.where(function () {
        for (const k of chunk) {
          const vals = JSON.parse(k);
          this.orWhere(function () {
            pkColumns.forEach((c, i) => this.andWhere(c, vals[i]));
          });
        }
      });
      rows = await b;
    }
    allRows.push(...rows);
  }

  const insertChunk = 100;
  for (let i = 0; i < allRows.length; i += insertChunk) {
    const rows = allRows.slice(i, i + insertChunk);
    try {
      await target.db(table).insert(rows).onConflict(pkColumns).ignore();
      copied += rows.length;
    } catch {
      for (const row of rows) {
        try {
          await target.db(table).insert(row).onConflict(pkColumns).ignore();
          copied++;
        } catch (err) {
          if (FK_RE.test(String(err.message))) {
            deferred.push(row);
          } else {
            errors.push({ pk: pkKey(row, pkColumns), err: err.message });
          }
        }
      }
    }
  }

  return { copied, deferred, errors };
}

/* ------------------------------------------------------------------ */
/* Clean sym_change_log: relabel mislabeled entries                   */
/* ------------------------------------------------------------------ */

async function cleanLog(node, knownIds) {
  const bad = await node
    .db("sym_change_log")
    .select("node_source_id")
    .count("* as cnt")
    .whereNot("node_source_id", node.id)
    .groupBy("node_source_id");

  const entries = [];
  for (const row of bad) {
    const srcId = row.node_source_id;
    const cnt = Number(row.cnt);
    if (!knownIds.has(srcId) || cnt === 0) continue;
    entries.push({ from: srcId, count: cnt });
  }

  if (entries.length === 0) return [];

  for (const e of entries) {
    if (!APPLY) {
      logger.info(
        `  [clean-log ${node.id}] (dry-run) would relabel ${e.count} entries: ${e.from} -> ${node.id}`,
      );
      continue;
    }
    const n = await node
      .db("sym_change_log")
      .where("node_source_id", e.from)
      .update({ node_source_id: node.id });
    logger.info(
      `  [clean-log ${node.id}] relabeled ${n} entries: ${e.from} -> ${node.id}`,
    );
    e.relabeled = n;
  }
  return entries;
}

/* ------------------------------------------------------------------ */
/* Analyze one branch without writing anything                        */
/* ------------------------------------------------------------------ */

async function analyzeBranch(hub, branch, tables, knownIds) {
  const report = {
    id: branch.id,
    logMislabeled: [],
    hubToBranch: [],
    branchToHub: [],
    errors: [],
  };

  // 1. Log analysis
  try {
    const bad = await branch
      .db("sym_change_log")
      .select("node_source_id")
      .count("* as cnt")
      .whereNot("node_source_id", branch.id)
      .groupBy("node_source_id");
    for (const row of bad) {
      const srcId = row.node_source_id;
      const cnt = Number(row.cnt);
      if (knownIds.has(srcId) && cnt > 0) {
        report.logMislabeled.push({ from: srcId, count: cnt });
      }
    }
  } catch (err) {
    report.errors.push(`log analysis: ${err.message}`);
  }

  // 2. Directional analysis
  for (const table of tables) {
    try {
      const h2b = await findMissing(hub, branch, table);
      if (h2b.missing.length > 0) {
        report.hubToBranch.push({ table, count: h2b.missing.length });
      }
    } catch (err) {
      report.errors.push(`hub->branch [${table}]: ${err.message}`);
    }
    try {
      const b2h = await findMissing(branch, hub, table);
      if (b2h.missing.length > 0) {
        report.branchToHub.push({ table, count: b2h.missing.length });
      }
    } catch (err) {
      report.errors.push(`branch->hub [${table}]: ${err.message}`);
    }
  }

  return report;
}

function branchNeedsFix(report) {
  return (
    report.logMislabeled.length > 0 ||
    report.hubToBranch.length > 0 ||
    report.branchToHub.length > 0 ||
    report.errors.length > 0
  );
}

function printReport(report) {
  const needsFix = branchNeedsFix(report);
  const tag = needsFix ? "NEEDS FIX" : "OK";

  logger.info(`\n=== Branch [${report.id}] — ${tag} ===`);

  if (!needsFix) {
    logger.info("  No issues found.");
    return;
  }

  if (report.logMislabeled.length > 0) {
    logger.info("  Mislabeled sym_change_log entries:");
    for (const m of report.logMislabeled) {
      logger.info(
        `    - ${m.count} entries labeled "${m.from}" (should be "${report.id}")`,
      );
    }
  }

  if (report.hubToBranch.length > 0) {
    const total = report.hubToBranch.reduce((s, e) => s + e.count, 0);
    logger.info(
      `  hub -> branch: ${total} rows across ${report.hubToBranch.length} table(s)`,
    );
    for (const e of report.hubToBranch) {
      logger.info(`    - ${e.table}: ${e.count}`);
    }
  }

  if (report.branchToHub.length > 0) {
    const total = report.branchToHub.reduce((s, e) => s + e.count, 0);
    logger.info(
      `  branch -> hub: ${total} rows across ${report.branchToHub.length} table(s)`,
    );
    for (const e of report.branchToHub) {
      logger.info(`    - ${e.table}: ${e.count}`);
    }
  }

  if (report.errors.length > 0) {
    logger.error(`  Errors during analysis:`);
    for (const e of report.errors) {
      logger.error(`    - ${e}`);
    }
  }
}

/* ------------------------------------------------------------------ */
/* Apply fixes for one branch                                         */
/* ------------------------------------------------------------------ */

async function applyBranch(hub, branch, tables, knownIds) {
  logger.info(`\n========== Applying to [${branch.id}] ==========`);

  // Phase 1: clean log
  logger.info(`--- Phase 1: relabel sym_change_log ---`);
  await cleanLog(branch, knownIds);

  // Phase 2: hub -> branch
  logger.info(`--- Phase 2: hub -> ${branch.id} ---`);
  const deferredHubToBranch = new Map();
  for (const table of tables) {
    try {
      const { missing, tgtCols } = await findMissing(hub, branch, table);
      if (missing.length === 0) continue;
      const res = await copyTable(hub, branch, table, missing, tgtCols);
      logger.info(
        `  [${table}] missing=${missing.length} copied=${res.copied} deferred=${res.deferred.length} errors=${res.errors.length}`,
      );
      if (res.deferred.length > 0) deferredHubToBranch.set(table, res.deferred);
      if (res.errors.length > 0) {
        res.errors.forEach((e) => logger.error(`    ${e.pk}: ${e.err}`));
      }
    } catch (err) {
      logger.error(`  [${table}] ERROR: ${err.message}`);
    }
  }

  // Phase 3: branch -> hub
  logger.info(`--- Phase 3: ${branch.id} -> hub ---`);
  const deferredBranchToHub = new Map();
  for (const table of tables) {
    try {
      const { missing, tgtCols } = await findMissing(branch, hub, table);
      if (missing.length === 0) continue;
      const res = await copyTable(branch, hub, table, missing, tgtCols);
      logger.info(
        `  [${table}] missing=${missing.length} copied=${res.copied} deferred=${res.deferred.length} errors=${res.errors.length}`,
      );
      if (res.deferred.length > 0) deferredBranchToHub.set(table, res.deferred);
      if (res.errors.length > 0) {
        res.errors.forEach((e) => logger.error(`    ${e.pk}: ${e.err}`));
      }
    } catch (err) {
      logger.error(`  [${table}] ERROR: ${err.message}`);
    }
  }

  // Phase 4: retry deferred
  for (const [deferredMap, label, tgt] of [
    [deferredHubToBranch, "hub->branch", branch],
    [deferredBranchToHub, "branch->hub", hub],
  ]) {
    if (deferredMap.size === 0) continue;
    logger.info(`--- Phase 4: retrying deferred (${label}) ---`);
    for (const [table, rows] of deferredMap) {
      const cols = await pkCols(tgt, table);
      let resolved = 0,
        still = 0;
      for (const row of rows) {
        try {
          await tgt.db(table).insert(row).onConflict(cols).ignore();
          resolved++;
        } catch {
          still++;
        }
      }
      logger.info(`  [${table}] resolved=${resolved} still_deferred=${still}`);
    }
  }
}

/* ------------------------------------------------------------------ */
/* Main                                                              */
/* ------------------------------------------------------------------ */

async function main() {
  // 5-second abort window in APPLY mode
  if (APPLY) {
    await new Promise((r) => setTimeout(r, 5000));
  }

  const hub = buildHub();
  const branches = buildBranches(branchArg);

  // --list: just show configured branches and exit
  if (LIST) {
    logger.info(`Hub: [${hub.id}]`);
    logger.info(`Configured branches (${branches.length}):`);
    for (const b of branches) {
      logger.info(`  - ${b.id}`);
    }
    await hub.db.destroy().catch(() => {});
    await Promise.allSettled(branches.map((b) => b.db.destroy()));
    process.exit(0);
  }

  const knownIds = new Set([hub.id, ...branches.map((b) => b.id)]);
  const tables = sortByDependency([...ALLOWED_TABLES]);

  // ---- DRY RUN: analyze all, report which branches need fixing ----
  if (!APPLY) {
    logger.info("\nAnalyzing all branches (read-only)...");
    const reports = [];
    for (const branch of branches) {
      logger.info(`Analyzing [${branch.id}]...`);
      const report = await analyzeBranch(hub, branch, tables, knownIds);
      reports.push(report);
      printReport(report);
    }

    const needFix = reports.filter(branchNeedsFix);
    logger.info("\n=========================================");
    logger.info(" SUMMARY");
    logger.info("=========================================");
    logger.info(`Hub:        [${hub.id}]`);
    logger.info(`Branches:   ${branches.length}`);
    logger.info(`Need fix:   ${needFix.length}`);

    if (needFix.length === 0) {
      logger.info("\nAll branches are in parity. Nothing to do.");
      await hub.db.destroy().catch(() => {});
      await Promise.allSettled(branches.map((b) => b.db.destroy()));
      process.exit(0);
    }

    logger.warn("\nBranches that NEED FIX:");
    for (const r of needFix) {
      const h2b = r.hubToBranch.reduce((s, e) => s + e.count, 0);
      const b2h = r.branchToHub.reduce((s, e) => s + e.count, 0);
      const lbl = r.logMislabeled.reduce((s, e) => s + e.count, 0);
      logger.warn(
        `  - ${r.id}: hub->branch ${h2b} rows, branch->hub ${b2h} rows, ${lbl} mislabeled log entries`,
      );
    }

    logger.info("\nTo fix, run:");
    if (branchArg) {
      logger.info(`  node sync-fix.js --apply ${branchArg}`);
    } else {
      logger.info(`  node sync-fix.js --apply`);
      logger.info(`  (or one at a time:)`);
      for (const r of needFix) {
        logger.info(`  node sync-fix.js --apply ${r.id}`);
      }
    }

    await hub.db.destroy().catch(() => {});
    await Promise.allSettled(branches.map((b) => b.db.destroy()));
    process.exit(1); // gaps exist
  }

  // ---- APPLY: fix selected branches ----
  for (const branch of branches) {
    await applyBranch(hub, branch, tables, knownIds);
  }

  // ---- Verify ----
  logger.info(`\n========== Verify ==========`);
  let anyGap = false;
  for (const branch of branches) {
    for (const table of tables) {
      try {
        const h2b = await findMissing(hub, branch, table);
        const b2h = await findMissing(branch, hub, table);
        if (h2b.missing.length > 0 || b2h.missing.length > 0) {
          logger.warn(
            `[${branch.id}] ${table}: h->b=${h2b.missing.length} b->h=${b2h.missing.length}`,
          );
          anyGap = true;
        }
      } catch {
        /* skip */
      }
    }
  }

  await hub.db.destroy().catch(() => {});
  await Promise.allSettled(branches.map((b) => b.db.destroy()));

  if (anyGap) {
    logger.error("\nSome gaps remain. See warnings above.");
    process.exit(1);
  }
  logger.info("\nAll branches in parity. Done.");
  process.exit(0);
}

main().catch((err) => {
  logger.error(`[Fatal] ${err.stack || err.message}`);
  process.exit(2);
});
