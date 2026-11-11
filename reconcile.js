#!/usr/bin/env node
/**
 * reconcile.js — Fix operational-table drift across hub and branches.
 *
 * Two problems this solves:
 *
 *   1. "Local-only" tables (pos_session, cash_count, ...) should live on the
 *      branch where the terminal is, plus a hub aggregate for reporting.
 *      If drift has occurred (cross-store rows, missing rows), this script
 *      cleans it up in one shot.
 *
 *   2. Stale triggers on "local-only" tables keep filling sym_change_log with
 *      events the engine will ignore forever. This script drops them.
 *
 *   3. (Optional) Remove stale sym_change_log rows for those tables.
 *
 * Usage:
 *
 *   # Dry run — show what would happen, change nothing
 *   node reconcile.js
 *
 *   # Apply fixes for a specific table set
 *   node reconcile.js --tables=pos_session,cash_count --apply
 *
 *   # Apply everything the default set covers
 *   node reconcile.js --all --apply
 *
 *   # Only show what's on each node (no plan, no changes)
 *   node reconcile.js --inspect
 *
 * Defaults (env, all optional):
 *   LOCAL_ONLY_TABLES   comma-separated, default: pos_session,cash_count
 *   HUB_STORE_FILTER    SQL fragment to identify hub-owned rows, default: NULL
 *                       (leave unset — the hub keeps everything)
 *   BRANCH_STORE_KEY    column used to filter per-store on branches, default: store
 *   BRANCH_STORE_TABLE  table that maps record_id → name, default: store
 *   BRANCH_STORE_NAME   column used for the store name, default: name
 *
 * Exit codes: 0 ok, 1 drift found in dry-run, 2 fatal.
 */

require("dotenv").config();
const knex = require("knex");
const winston = require("winston");
const { URL } = require("url");
const readline = require("readline");

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

const opts = {
  tables: null,
  apply: false,
  inspect: false,
  all: false,
  skipDropTriggers: false,
  skipLogCleanup: false,
  yes: false,
};

for (const a of process.argv.slice(2)) {
  if (a === "--apply") opts.apply = true;
  else if (a === "--dry-run") opts.apply = false;
  else if (a === "--inspect") opts.inspect = true;
  else if (a === "--all") opts.all = true;
  else if (a === "--skip-drop-triggers") opts.skipDropTriggers = true;
  else if (a === "--skip-log-cleanup") opts.skipLogCleanup = true;
  else if (a === "--yes" || a === "-y") opts.yes = true;
  else if (a === "--help" || a === "-h") {
    console.log(`
Usage: node reconcile.js [options]

  --tables=T1,T2         tables to reconcile (default: LOCAL_ONLY_TABLES)
  --all                  reconcile every table in LOCAL_ONLY_TABLES
  --inspect              print current state only, no plan
  --apply                execute the plan (default: dry-run)
  --skip-drop-triggers   don't drop triggers on local-only tables
  --skip-log-cleanup     don't remove stale sym_change_log rows
  --yes                  skip the interactive confirmation
  --help                 show this help
`);
    process.exit(0);
  } else if (a.startsWith("--tables=")) {
    opts.tables = a
      .slice(9)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  } else {
    logger.error(`Unknown arg: ${a}`);
    process.exit(2);
  }
}

/* ------------------------------------------------------------------ */
/* Config from env                                                    */
/* ------------------------------------------------------------------ */

function csv(s, fallback) {
  const v = (s || fallback || "").trim();
  return v
    ? v
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean)
    : [];
}

const LOCAL_ONLY_TABLES = csv(
  process.env.LOCAL_ONLY_TABLES,
  "pos_session,cash_count",
);
const BRANCH_STORE_KEY = process.env.BRANCH_STORE_KEY || "store";
const BRANCH_STORE_TABLE = process.env.BRANCH_STORE_TABLE || "store";
const BRANCH_STORE_NAME = process.env.BRANCH_STORE_NAME || "name";

if (!opts.tables) {
  opts.tables = LOCAL_ONLY_TABLES;
}

if (opts.all) {
  opts.tables = LOCAL_ONLY_TABLES;
}

if (opts.tables.length === 0) {
  logger.error(
    "No tables to reconcile. Set LOCAL_ONLY_TABLES=... in .env or pass --tables=",
  );
  process.exit(2);
}

if (!opts.apply && !opts.inspect) {
  logger.info(
    "==================================================================\n" +
      "  DRY RUN MODE — no writes will happen.\n" +
      "  Re-run with --apply to execute.\n" +
      "==================================================================",
  );
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

function parseConnectionString(urlStr, defaultPort = 3306) {
  if (!urlStr) throw new Error("empty connection string");
  const u = new URL(urlStr);
  return {
    host: u.hostname || "127.0.0.1",
    port: u.port ? parseInt(u.port, 10) : defaultPort,
    user: u.username ? decodeURIComponent(u.username) : undefined,
    password: u.password ? decodeURIComponent(u.password) : undefined,
    database: u.pathname.slice(1),
  };
}

function buildKnex(client, url) {
  if (client === "pg" || client === "postgres") {
    return knex({
      client: "pg",
      connection: { connectionString: url, ssl: false },
      pool: { min: 0, max: 4 },
    });
  }
  return knex({
    client: "mysql2",
    connection: parseConnectionString(url),
    pool: { min: 0, max: 4 },
  });
}

function resolveNodes() {
  const byId = new Map();
  const hubId = process.env.LOCAL_ID || "NODE_MAIN_HUB";
  if (process.env.LOCAL_URL) {
    const hub = {
      id: hubId,
      client: normalizeClient(process.env.LOCAL_CLIENT),
      url: process.env.LOCAL_URL,
      isHub: true,
    };
    byId.set(hubId, hub);
    byId.set("hub", hub);
  }
  let branches = [];
  try {
    branches = JSON.parse(process.env.BRANCHES_JSON || "[]");
  } catch (err) {
    logger.error(`Failed to parse BRANCHES_JSON: ${err.message}`);
    process.exit(2);
  }
  for (const b of branches) {
    byId.set(b.id, {
      id: b.id,
      client: normalizeClient(b.client),
      url: b.url,
      isHub: false,
    });
  }
  return byId;
}

/* ------------------------------------------------------------------ */
/* Table inspection                                                   */
/* ------------------------------------------------------------------ */

async function tableExists(db, table) {
  try {
    await db(table).limit(1);
    return true;
  } catch {
    return false;
  }
}

async function getStoreForBranch(db, branchName) {
  // Best-effort: try to find a store matching the branch name pattern.
  // Fallback: return null and let the caller decide.
  try {
    const exact = await db(BRANCH_STORE_TABLE)
      .where(BRANCH_STORE_NAME, branchName)
      .first();
    if (exact) return exact.record_id;
    // Try case-insensitive / partial match
    const like = await db(BRANCH_STORE_TABLE)
      .whereRaw(`UPPER(${BRANCH_STORE_NAME}) LIKE ?`, [
        `%${branchName.toUpperCase()}%`,
      ])
      .first();
    if (like) return like.record_id;
  } catch {
    /* ignore */
  }
  return null;
}

async function inspectTable(db, table) {
  try {
    const count = await db(table).count("* as c").first();
    const total = Number(count?.c || 0);
    let perStore = null;
    try {
      const rows = await db(table)
        .join(
          BRANCH_STORE_TABLE,
          `${table}.${BRANCH_STORE_KEY}`,
          `${BRANCH_STORE_TABLE}.record_id`,
        )
        .select(`${BRANCH_STORE_TABLE}.${BRANCH_STORE_NAME} as store_name`)
        .count("* as c")
        .groupBy(`${BRANCH_STORE_TABLE}.${BRANCH_STORE_NAME}`);
      perStore = rows.map((r) => ({
        store: r.store_name,
        count: Number(r.c),
      }));
    } catch {
      perStore = null;
    }
    return { total, perStore };
  } catch (err) {
    return { total: null, perStore: null, error: err.message };
  }
}

/* ------------------------------------------------------------------ */
/* Plan                                                               */
/* ------------------------------------------------------------------ */

async function buildPlan(node, table) {
  const db = node.db;
  const plan = { node: node.id, table, actions: [] };

  if (!(await tableExists(db, table))) {
    plan.actions.push({ kind: "skip", reason: "table does not exist" });
    return plan;
  }

  // Which store owns this node?
  let ownedStoreId = null;
  if (!node.isHub) {
    // Branch: derive store name from node id (NODE_BRANCH_02 → BRANCH 02)
    // and look it up. This matches the naming used by node-copy --new-branch.
    const m = node.id.match(/BRANCH[_\s-]*(\d+)/i);
    const storeNameGuess = m ? `BRANCH ${m[1]}` : node.id;
    ownedStoreId =
      (await getStoreForBranch(db, storeNameGuess)) ||
      (await getStoreForBranch(db, node.id));
    if (!ownedStoreId) {
      plan.actions.push({
        kind: "warn",
        reason: `could not determine owned store for ${node.id} (tried "${storeNameGuess}")`,
      });
    }
  }

  // For branches: find rows that don't belong to the owned store
  if (!node.isHub && ownedStoreId) {
    const foreign = await db(table)
      .whereNot(BRANCH_STORE_KEY, ownedStoreId)
      .count("* as c")
      .first();
    const foreignCount = Number(foreign?.c || 0);
    if (foreignCount > 0) {
      plan.actions.push({
        kind: "delete",
        count: foreignCount,
        where: `${BRANCH_STORE_KEY} != '${ownedStoreId}'`,
      });
    }
  }

  // For all nodes: drop triggers on this table
  if (!opts.skipDropTriggers) {
    try {
      const trigs = await db.raw(
        `SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
         WHERE TRIGGER_SCHEMA=DATABASE() AND TRIGGER_NAME LIKE ?`,
        [`sym_trig_${table}_%`],
      );
      const list = (trigs[0] || trigs).map((r) => r.TRIGGER_NAME);
      if (list.length > 0) {
        plan.actions.push({ kind: "drop_triggers", triggers: list });
      }
    } catch (err) {
      plan.actions.push({
        kind: "warn",
        reason: `could not list triggers: ${err.message}`,
      });
    }
  }

  // Stale log rows
  if (!opts.skipLogCleanup) {
    try {
      const logCount = await db("sym_change_log")
        .where("table_name", table)
        .count("* as c")
        .first();
      const n = Number(logCount?.c || 0);
      if (n > 0) {
        plan.actions.push({ kind: "clear_log", count: n });
      }
    } catch {
      /* ignore */
    }
  }

  return plan;
}

/* ------------------------------------------------------------------ */
/* Execute                                                            */
/* ------------------------------------------------------------------ */

async function executeAction(node, table, action) {
  const db = node.db;

  if (action.kind === "delete") {
    // Use the same predicate we computed in plan (safe: it's a boolean expr
    // on a single column, not user input).
    const whereExpr = action.where;
    logger.info(`  [${node.id}] ${table}: deleting rows where ${whereExpr}`);
    const n = await db(table).whereRaw(whereExpr).del();
    logger.info(`  [${node.id}] ${table}: deleted ${n} row(s)`);
    return;
  }

  if (action.kind === "drop_triggers") {
    for (const t of action.triggers) {
      logger.info(`  [${node.id}] dropping trigger ${t}`);
      await db.raw(`DROP TRIGGER IF EXISTS \`${t}\``);
    }
    return;
  }

  if (action.kind === "clear_log") {
    logger.info(
      `  [${node.id}] clearing ${action.count} stale sym_change_log row(s) for ${table}`,
    );
    const n = await db("sym_change_log").where("table_name", table).del();
    logger.info(`  [${node.id}] cleared ${n} row(s)`);
    return;
  }

  if (action.kind === "warn") {
    logger.warn(`  [${node.id}] ${table}: ${action.reason}`);
    return;
  }

  if (action.kind === "skip") {
    logger.info(`  [${node.id}] ${table}: ${action.reason} — skipped`);
    return;
  }
}

/* ------------------------------------------------------------------ */
/* Interactive confirmation                                           */
/* ------------------------------------------------------------------ */

function ask(q) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  return new Promise((res) =>
    rl.question(q, (a) => {
      rl.close();
      res(a.trim().toLowerCase());
    }),
  );
}

/* ------------------------------------------------------------------ */
/* Main                                                               */
/* ------------------------------------------------------------------ */

async function main() {
  const nodes = resolveNodes();
  if (nodes.size === 0) {
    logger.error("No nodes configured (LOCAL_URL / BRANCHES_JSON missing)");
    process.exit(2);
  }

  // Deduplicate (hub + hub alias)
  const nodeList = [];
  const seen = new Set();
  for (const [id, n] of nodes) {
    if (id === "hub") continue;
    if (seen.has(n.id)) continue;
    seen.add(n.id);
    nodeList.push(n);
  }

  logger.info(
    `Reconciling tables: ${opts.tables.join(", ")}\n` +
      `Nodes: ${nodeList.map((n) => `[${n.id}]`).join(", ")}\n` +
      `Mode: ${opts.inspect ? "INSPECT" : opts.apply ? "APPLY" : "DRY RUN"}`,
  );

  // Open connections
  for (const n of nodeList) {
    n.db = buildKnex(n.client, n.url);
  }

  try {
    for (const table of opts.tables) {
      logger.info(`\n--- ${table} ---`);

      // Inspect current state
      for (const n of nodeList) {
        const info = await inspectTable(n.db, table);
        if (info.error) {
          logger.warn(`  [${n.id}] ${info.error}`);
          continue;
        }
        const per = info.perStore
          ? ` (${info.perStore.map((s) => `${s.store}:${s.count}`).join(", ")})`
          : "";
        logger.info(`  [${n.id}] ${info.total} row(s)${per}`);
      }

      if (opts.inspect) continue;

      // Build plans
      const plans = [];
      for (const n of nodeList) {
        const p = await buildPlan(n, table);
        if (p.actions.length > 0) plans.push(p);
      }

      if (plans.length === 0) {
        logger.info(`  no actions needed on ${table}`);
        continue;
      }

      // Show plan
      logger.info(`  Plan:`);
      for (const p of plans) {
        for (const a of p.actions) {
          if (a.kind === "delete") {
            logger.info(
              `    [${p.node}] DELETE ${a.count} row(s) where ${a.where}`,
            );
          } else if (a.kind === "drop_triggers") {
            logger.info(
              `    [${p.node}] DROP ${a.triggers.length} trigger(s) for ${table}`,
            );
          } else if (a.kind === "clear_log") {
            logger.info(
              `    [${p.node}] CLEAR ${a.count} sym_change_log row(s) for ${table}`,
            );
          } else if (a.kind === "warn") {
            logger.warn(`    [${p.node}] ${a.reason}`);
          } else if (a.kind === "skip") {
            logger.info(`    [${p.node}] ${a.reason}`);
          }
        }
      }

      if (!opts.apply) {
        logger.info(`  (dry-run — not applied)`);
        continue;
      }

      // Confirm
      if (!opts.yes) {
        const answer = await ask(
          `\nApply ${plans.reduce((s, p) => s + p.actions.length, 0)} action(s) for ${table}? Type "yes": `,
        );
        if (answer !== "yes") {
          logger.warn(`  skipped`);
          continue;
        }
      }

      // Apply
      for (const p of plans) {
        const node = nodeList.find((n) => n.id === p.node);
        for (const a of p.actions) {
          try {
            await executeAction(node, table, a);
          } catch (err) {
            logger.error(`    [${p.node}] action failed: ${err.message}`);
          }
        }
      }
    }
  } finally {
    for (const n of nodeList) {
      await n.db.destroy().catch(() => {});
    }
  }

  logger.info("\nDone.");
}

main()
  .then((code) => process.exit(code ?? 0))
  .catch((err) => {
    logger.error(`[Fatal] ${err.stack || err.message}`);
    process.exit(2);
  });
