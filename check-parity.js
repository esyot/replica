#!/usr/bin/env node
/**
 * check-parity.js — read-only data verification between hub and branches.
 *
 * For each allowed table, compares PKs (and optionally row contents) between
 * the hub and each branch. Reports:
 *   - missing on branch (hub has it, branch doesn't)
 *   - missing on hub    (branch has it, hub doesn't)
 *   - row diff          (same PK, different column values) — only with --full
 *
 * Usage:
 *   node check-parity.js                       # all branches, PK-existence only
 *   node check-parity.js NODE_BRANCH_02        # one branch
 *   node check-parity.js --full                # also compare row contents
 *   node check-parity.js --table=product       # restrict to one table
 *   node check-parity.js --limit=1000          # sample N rows per table
 *   node check-parity.js --json                # machine-readable output
 *
 * Exit codes:
 *   0 = parity ok (no missing / no diff beyond --max-report)
 *   1 = parity mismatch
 *   2 = runtime error
 *
 * READ-ONLY. Never writes to sym_change_log / sym_checkpoint / data tables.
 */

require("dotenv").config();
const knex = require("knex");
const winston = require("winston");

const { getTablePkColumns, ALLOWED_TABLES } = require("./triggers");

/* ------------------------------------------------------------------ */
/* Logger                                                             */
/* ------------------------------------------------------------------ */

const logger = winston.createLogger({
  level: process.env.NODE_ENV === "production" ? "info" : "debug",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      ({ timestamp, level, message }) =>
        `[${timestamp}] [${level.toUpperCase()}]: ${message}`,
    ),
  ),
  transports: [new winston.transports.Console({ silent: false })],
});

/* ------------------------------------------------------------------ */
/* CLI parsing                                                        */
/* ------------------------------------------------------------------ */

const argv = process.argv.slice(2);
const opts = {
  branchIds: [],
  full: false,
  table: null,
  limit: null,
  json: false,
  maxReport: 50,
};

for (const a of argv) {
  if (a === "--full") opts.full = true;
  else if (a === "--json") opts.json = true;
  else if (a.startsWith("--table=")) opts.table = a.slice(8);
  else if (a.startsWith("--limit=")) opts.limit = parseInt(a.slice(8), 10);
  else if (a.startsWith("--max-report="))
    opts.maxReport = parseInt(a.slice(13), 10);
  else if (!a.startsWith("-")) opts.branchIds.push(a);
}

/* ------------------------------------------------------------------ */
/* Env helpers (same conventions as force-sync.js)                    */
/* ------------------------------------------------------------------ */

function parseIntEnv(name, def, { min, max } = {}) {
  const raw = process.env[name];
  let v = raw !== undefined && raw !== "" ? parseInt(raw, 10) : def;
  if (Number.isNaN(v)) v = def;
  if (min !== undefined) v = Math.max(min, v);
  if (max !== undefined) v = Math.min(max, v);
  return v;
}

function parseBoolEnv(name, def = false) {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return def;
  return raw === "true" || raw === "1";
}

function normalizeClient(c) {
  if (!c) return "mysql2";
  c = c.toLowerCase().trim();
  if (c === "mysql") return "mysql2";
  if (c === "postgresql") return "pg";
  return c;
}

function isPgClient(c) {
  return c === "pg" || c === "postgres";
}

function parseConnectionString(urlStr, defaultPort = 3306) {
  const p = new URL(urlStr);
  return {
    host: p.hostname || "127.0.0.1",
    port: p.port ? parseInt(p.port, 10) : defaultPort,
    user: p.username ? decodeURIComponent(p.username) : undefined,
    password: p.password ? decodeURIComponent(p.password) : undefined,
    database: p.pathname.slice(1),
  };
}

function buildKnex(url, clientName, ssl, sslStrict) {
  if (isPgClient(clientName)) {
    return knex({
      client: clientName,
      connection: {
        connectionString: url,
        ssl: ssl ? { rejectUnauthorized: sslStrict !== false } : false,
      },
      pool: { min: 0, max: parseIntEnv("PARITY_POOL_MAX", 4, { min: 1 }) },
    });
  }
  return knex({
    client: clientName,
    connection: parseConnectionString(url, 3306),
    pool: { min: 0, max: parseIntEnv("PARITY_POOL_MAX", 4, { min: 1 }) },
  });
}

/* ------------------------------------------------------------------ */
/* Build nodes                                                        */
/* ------------------------------------------------------------------ */

function buildHub() {
  const client = normalizeClient(process.env.LOCAL_CLIENT);
  return {
    id: process.env.LOCAL_ID || "NODE_MAIN_HUB",
    client,
    db: buildKnex(
      process.env.LOCAL_URL,
      client,
      parseBoolEnv("LOCAL_SSL", false),
      parseBoolEnv("LOCAL_SSL_STRICT", true),
    ),
  };
}

function buildBranches(filterIds) {
  const cfg = JSON.parse(process.env.BRANCHES_JSON || "[]");
  const list =
    filterIds.length > 0 ? cfg.filter((b) => filterIds.includes(b.id)) : cfg;
  if (filterIds.length > 0 && list.length !== filterIds.length) {
    const found = new Set(list.map((b) => b.id));
    const missing = filterIds.filter((id) => !found.has(id));
    logger.error(`Unknown branch id(s): ${missing.join(", ")}`);
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
/* PK cache (per node+table)                                          */
/* ------------------------------------------------------------------ */

const PK_CACHE = new Map();
async function pkCols(node, table) {
  if (!ALLOWED_TABLES.has(table)) throw new Error(`disallowed table: ${table}`);
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

/* ------------------------------------------------------------------ */
/* PK normalization — must match what your triggers encode           */
/* ------------------------------------------------------------------ */

/**
 * Build a stable string key from a row's PK values, matching the format
 * used by pk-codec.js for single/composite keys.
 *   single  -> String(val)
 *   composite -> JSON array string
 */
function pkKeyFromRow(row, cols) {
  if (cols.length === 1) {
    const v = row[cols[0]];
    return v == null ? null : String(v);
  }
  return JSON.stringify(cols.map((c) => row[c] ?? null));
}

/* ------------------------------------------------------------------ */
/* Table comparison                                                   */
/* ------------------------------------------------------------------ */

async function compareTable(hub, branch, table) {
  const hubCols = await pkCols(hub, table);
  const branchCols = await pkCols(branch, table);

  // Sanity: both nodes must agree on PK columns for a fair comparison.
  if (hubCols.join(",") !== branchCols.join(",")) {
    return {
      table,
      error: `PK mismatch: hub=[${hubCols}] branch=[${branchCols}]`,
    };
  }

  const hubQuery = hub.db(table).select(hubCols);
  const branchQuery = branch.db(table).select(branchCols);
  if (opts.limit) {
    hubQuery.limit(opts.limit);
    branchQuery.limit(opts.limit);
  }

  const [hubRows, branchRows] = await Promise.all([hubQuery, branchQuery]);

  const hubKeys = new Set();
  const branchKeys = new Set();
  for (const r of hubRows) {
    const k = pkKeyFromRow(r, hubCols);
    if (k != null) hubKeys.add(k);
  }
  for (const r of branchRows) {
    const k = pkKeyFromRow(r, branchCols);
    if (k != null) branchKeys.add(k);
  }

  const missingOnBranch = [];
  for (const k of hubKeys) if (!branchKeys.has(k)) missingOnBranch.push(k);

  const missingOnHub = [];
  for (const k of branchKeys) if (!hubKeys.has(k)) missingOnHub.push(k);

  const result = {
    table,
    hubRowCount: hubRows.length,
    branchRowCount: branchRows.length,
    missingOnBranch: missingOnBranch.length,
    missingOnHub: missingOnHub.length,
    samples: {
      missingOnBranch: missingOnBranch.slice(0, opts.maxReport),
      missingOnHub: missingOnHub.slice(0, opts.maxReport),
    },
    diffs: 0,
    diffSamples: [],
  };

  // Optional: content diff for rows present on both sides.
  if (opts.full) {
    const bothKeys = [...hubKeys].filter((k) => branchKeys.has(k));
    // Re-fetch only the shared rows (with all columns) — chunked.
    const chunk = 200;
    let diffCount = 0;
    for (let i = 0; i < bothKeys.length; i += chunk) {
      const keysChunk = bothKeys.slice(i, i + chunk);

      const hubWhere =
        hubCols.length === 1 ? { col: hubCols[0], vals: keysChunk } : null;

      let hRows, bRows;
      if (hubCols.length === 1) {
        hRows = await hub.db(table).whereIn(hubCols[0], keysChunk);
        bRows = await branch.db(table).whereIn(branchCols[0], keysChunk);
      } else {
        // Composite: fetch whole table limited to bothKeys via OR groups.
        hRows = await fetchComposite(hub.db, table, hubCols, keysChunk);
        bRows = await fetchComposite(branch.db, table, branchCols, keysChunk);
      }

      const hMap = new Map(hRows.map((r) => [pkKeyFromRow(r, hubCols), r]));
      const bMap = new Map(bRows.map((r) => [pkKeyFromRow(r, branchCols), r]));

      for (const k of keysChunk) {
        const h = hMap.get(k);
        const b = bMap.get(k);
        if (!h || !b) continue;
        const cols = new Set([...Object.keys(h), ...Object.keys(b)]);
        const differences = [];
        for (const c of cols) {
          if (hubCols.includes(c)) continue; // PKs already matched
          const hv = normalizeForCompare(h[c]);
          const bv = normalizeForCompare(b[c]);
          if (hv !== bv)
            differences.push({ column: c, hub: h[c], branch: b[c] });
        }
        if (differences.length > 0) {
          diffCount++;
          if (result.diffSamples.length < opts.maxReport) {
            result.diffSamples.push({ pk: k, differences });
          }
        }
      }
    }
    result.diffs = diffCount;
  }

  return result;
}

async function fetchComposite(db, table, cols, keys) {
  const builder = db(table);
  builder.where(function () {
    for (const k of keys) {
      const vals = JSON.parse(k);
      this.orWhere(function () {
        cols.forEach((c, i) => this.andWhere(c, vals[i]));
      });
    }
  });
  return builder;
}

function normalizeForCompare(v) {
  if (v == null) return "null";
  if (v instanceof Date) return v.toISOString();
  if (Buffer.isBuffer(v)) return v.toString("base64");
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/* ------------------------------------------------------------------ */
/* Main                                                              */
/* ------------------------------------------------------------------ */

async function main() {
  const hub = buildHub();
  const branches = buildBranches(opts.branchIds);
  if (branches.length === 0) {
    logger.error("No branches configured.");
    process.exit(2);
  }

  const tables = opts.table
    ? [opts.table].filter((t) => ALLOWED_TABLES.has(t))
    : [...ALLOWED_TABLES];

  if (opts.table && tables.length === 0) {
    logger.error(`--table=${opts.table} is not in ALLOWED_TABLES`);
    process.exit(2);
  }

  logger.info(
    `Parity check: hub=[${hub.id}] branches=[${branches.map((b) => b.id).join(", ")}] ` +
      `tables=${tables.length} full=${opts.full} limit=${opts.limit ?? "none"}`,
  );

  const allReports = [];
  let anyMismatch = false;

  for (const branch of branches) {
    logger.info(`--- branch [${branch.id}] ---`);
    const branchReport = { branchId: branch.id, tables: [], ok: true };

    for (const table of tables) {
      try {
        const r = await compareTable(hub, branch, table);
        branchReport.tables.push(r);
        const bad =
          r.error || r.missingOnBranch > 0 || r.missingOnHub > 0 || r.diffs > 0;
        if (bad) branchReport.ok = false;

        if (!opts.json) {
          if (r.error) {
            logger.error(`[${table}] ${r.error}`);
          } else if (!bad) {
            logger.debug(
              `[${table}] ok (hub=${r.hubRowCount} branch=${r.branchRowCount})`,
            );
          } else {
            logger.warn(
              `[${table}] hub=${r.hubRowCount} branch=${r.branchRowCount} ` +
                `missingOnBranch=${r.missingOnBranch} missingOnHub=${r.missingOnHub}` +
                (opts.full ? ` diffs=${r.diffs}` : ""),
            );
            if (r.samples.missingOnBranch.length) {
              logger.warn(
                `  missingOnBranch sample: ${JSON.stringify(r.samples.missingOnBranch)}`,
              );
            }
            if (r.samples.missingOnHub.length) {
              logger.warn(
                `  missingOnHub sample: ${JSON.stringify(r.samples.missingOnHub)}`,
              );
            }
            if (r.diffSamples.length) {
              logger.warn(
                `  diff sample: ${JSON.stringify(r.diffSamples[0], null, 2)}`,
              );
            }
          }
        }
      } catch (err) {
        branchReport.ok = false;
        branchReport.tables.push({ table, error: err.message });
        logger.error(`[${table}] compare failed: ${err.message}`);
      }
    }

    if (!branchReport.ok) anyMismatch = true;
    allReports.push(branchReport);
  }

  if (opts.json) {
    // eslint-disable-next-line no-console
    console.log(JSON.stringify(allReports, null, 2));
  }

  await hub.db.destroy().catch(() => {});
  await Promise.allSettled(branches.map((b) => b.db.destroy()));

  if (anyMismatch) {
    logger.error("Parity check: MISMATCHES FOUND.");
    process.exit(1);
  }
  logger.info("Parity check: all tables in parity.");
  process.exit(0);
}

main().catch((err) => {
  logger.error(`[Fatal] ${err.stack || err.message}`);
  process.exit(2);
});
