#!/usr/bin/env node
/**
 * backfill.js — copy missing rows between hub and branches.
 *
 * Modes:
 *   --direction=hub-to-branch    Fill rows hub has that branch doesn't
 *   --direction=branch-to-hub    Fill rows branch has that hub doesn't
 *   --direction=both             Do both (requires --confirm)
 *
 * Which rows:
 *   Default: compare PKs; copy whatever is missing on the target.
 *   --pk-file=path               Only copy PKs listed in this file (one per line)
 *   --since=YYYY-MM-DD           Only consider source rows whose created_at >= date
 *
 * Safety:
 *   --dry-run (default)          Report only; change nothing
 *   --apply                      Actually write to the target
 *   --branch=ID                  Restrict to a single branch
 *   --tables=t1,t2               Restrict to specific tables
 *   --max-rows=N                 Refuse per-table if would copy > N (default 50000)
 *
 * Exit codes:
 *   0 = all requested copies succeeded
 *   1 = some copies failed
 *   2 = runtime error / refused (safety guard)
 */

require("dotenv").config();
const knex = require("knex");
const winston = require("winston");
const fs = require("fs");

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
const opts = {
  direction: null,
  branch: null,
  tables: null,
  apply: false,
  maxRows: 50000,
  pkFile: null,
  since: null,
  confirm: false,
};

for (const a of argv) {
  if (a === "--apply") opts.apply = true;
  else if (a === "--confirm") opts.confirm = true;
  else if (a === "--dry-run") opts.apply = false;
  else if (a.startsWith("--direction=")) opts.direction = a.slice(12);
  else if (a.startsWith("--branch=")) opts.branch = a.slice(9);
  else if (a.startsWith("--tables="))
    opts.tables = a
      .slice(9)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  else if (a.startsWith("--max-rows="))
    opts.maxRows = parseInt(a.slice(11), 10);
  else if (a.startsWith("--pk-file=")) opts.pkFile = a.slice(10);
  else if (a.startsWith("--since=")) opts.since = a.slice(8);
  else {
    logger.error(`Unknown arg: ${a}`);
    process.exit(2);
  }
}

if (
  !opts.direction ||
  !["hub-to-branch", "branch-to-hub", "both"].includes(opts.direction)
) {
  logger.error("--direction required: hub-to-branch | branch-to-hub | both");
  process.exit(2);
}

if (opts.direction === "both" && !opts.confirm) {
  logger.error(
    "--direction=both requires --confirm (it's bidirectional and can loop if misused)",
  );
  process.exit(2);
}

/* ------------------------------------------------------------------ */
/* Env / connections                                                  */
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

function parseConn(url, defaultPort = 3306) {
  const p = new URL(url);
  return {
    host: p.hostname || "127.0.0.1",
    port: p.port ? parseInt(p.port, 10) : defaultPort,
    user: p.username ? decodeURIComponent(p.username) : undefined,
    password: p.password ? decodeURIComponent(p.password) : undefined,
    database: p.pathname.slice(1),
  };
}

function buildKnex(url, client, ssl, sslStrict) {
  if (isPgClient(client)) {
    return knex({
      client,
      connection: {
        connectionString: url,
        ssl: ssl ? { rejectUnauthorized: sslStrict !== false } : false,
      },
      pool: { min: 0, max: 4 },
    });
  }
  return knex({
    client,
    connection: parseConn(url, 3306),
    pool: { min: 0, max: 4 },
  });
}

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

function buildBranches(filterId) {
  const cfg = JSON.parse(process.env.BRANCHES_JSON || "[]");
  const list = filterId ? cfg.filter((b) => b.id === filterId) : cfg;
  if (filterId && list.length === 0) {
    logger.error(`Unknown branch: ${filterId}`);
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

function pkKeyFromRow(row, cols) {
  if (cols.length === 1) {
    const v = row[cols[0]];
    return v == null ? null : String(v);
  }
  return JSON.stringify(cols.map((c) => row[c] ?? null));
}

function pkWhereFromKey(k, cols) {
  if (cols.length === 1) return { [cols[0]]: k };
  const vals = JSON.parse(k);
  const where = {};
  cols.forEach((c, i) => (where[c] = vals[i]));
  return where;
}

/* ------------------------------------------------------------------ */
/* Discovery: what's missing?                                         */
/* ------------------------------------------------------------------ */

async function findMissing(source, target, table, sinceDate) {
  const srcCols = await pkCols(source, table);
  const tgtCols = await pkCols(target, table);
  if (srcCols.join(",") !== tgtCols.join(",")) {
    throw new Error(
      `PK mismatch on ${table}: src=[${srcCols}] tgt=[${tgtCols}]`,
    );
  }

  // Try to include created_at for --since filtering. Fall back if the
  // table doesn't have it (e.g. Laravel pivot tables like model_has_roles).
  let srcRows;
  try {
    srcRows = await source.db(table).select([...srcCols, "created_at"]);
  } catch (err) {
    if (/Unknown column 'created_at'/i.test(err.message)) {
      // Table has no created_at. Ignore --since for this table and warn.
      if (sinceDate) {
        logger.warn(
          `  [${table}] no created_at column — --since ignored for this table`,
        );
      }
      srcRows = await source.db(table).select(srcCols);
    } else {
      throw err;
    }
  }

  const tgtRows = await target.db(table).select(tgtCols);

  const tgtKeys = new Set();
  for (const r of tgtRows) {
    const k = pkKeyFromRow(r, tgtCols);
    if (k != null) tgtKeys.add(k);
  }

  const missing = [];
  for (const r of srcRows) {
    if (sinceDate && r.created_at) {
      const ca =
        r.created_at instanceof Date ? r.created_at : new Date(r.created_at);
      if (!Number.isNaN(ca.getTime()) && ca.getTime() < sinceDate.getTime()) {
        continue;
      }
    }
    const k = pkKeyFromRow(r, srcCols);
    if (k != null && !tgtKeys.has(k)) missing.push(k);
  }

  return { missing, srcCols, tgtCols, srcRowCount: srcRows.length };
}

/* ------------------------------------------------------------------ */
/* Load a specific set of PKs from --pk-file                          */
/* ------------------------------------------------------------------ */

function loadPkFile(path) {
  if (!fs.existsSync(path)) {
    logger.error(`--pk-file not found: ${path}`);
    process.exit(2);
  }
  return new Set(
    fs
      .readFileSync(path, "utf8")
      .split(/\r?\n/)
      .map((s) => s.trim())
      .filter(Boolean),
  );
}

/* ------------------------------------------------------------------ */
/* Copy rows                                                          */
/* ------------------------------------------------------------------ */

async function copyRows(source, target, table, keys, pkColumns) {
  if (keys.length === 0) return { copied: 0, failed: 0, errors: [] };

  const errors = [];
  let copied = 0;
  let failed = 0;

  const chunkSize = 200;
  const insertChunk = 100;

  for (let i = 0; i < keys.length; i += chunkSize) {
    const chunk = keys.slice(i, i + chunkSize);

    let srcRows;
    try {
      if (pkColumns.length === 1) {
        srcRows = await source.db(table).whereIn(pkColumns[0], chunk);
      } else {
        const b = source.db(table);
        b.where(function () {
          for (const k of chunk) {
            const w = pkWhereFromKey(k, pkColumns);
            this.orWhere(function () {
              for (const [c, v] of Object.entries(w)) this.andWhere(c, v);
            });
          }
        });
        srcRows = await b;
      }
    } catch (err) {
      failed += chunk.length;
      errors.push({ chunk: i, error: `fetch: ${err.message}` });
      continue;
    }

    if (srcRows.length === 0) {
      errors.push({ chunk: i, error: "source returned 0 rows" });
      continue;
    }

    for (let j = 0; j < srcRows.length; j += insertChunk) {
      const rows = srcRows.slice(j, j + insertChunk);
      try {
        await target.db(table).insert(rows).onConflict(pkColumns).ignore();
        copied += rows.length;
      } catch (err) {
        failed += rows.length;
        errors.push({ chunk: `${i}+${j}`, error: `insert: ${err.message}` });
      }
    }
  }

  return { copied, failed, errors };
}

/* ------------------------------------------------------------------ */
/* Phantom-store pre-check (only for branch->hub)                     */
/* ------------------------------------------------------------------ */

async function phantomStoreCheck(source, target, table) {
  if (!["stocks_on_hand", "store_user", "pricing_scheme"].includes(table)) {
    return null;
  }
  try {
    const srcStores = await source
      .db(table)
      .distinct("store")
      .whereNotNull("store");
    const srcIds = srcStores.map((r) => r.store).filter(Boolean);
    if (srcIds.length === 0) return null;

    const tgt = await target
      .db("store")
      .select("record_id")
      .whereIn("record_id", srcIds);
    const tgtSet = new Set(tgt.map((r) => r.record_id));
    const phantom = srcIds.filter((id) => !tgtSet.has(id));
    if (phantom.length === 0) return null;

    return {
      table,
      phantomStores: phantom,
      message:
        `Table "${table}" references ${phantom.length} store(s) that do not exist on the target. ` +
        `FK checks will fail unless those stores are copied first. ` +
        `Consider also backfilling "store" for these PKs.`,
    };
  } catch {
    return null;
  }
}

/* ------------------------------------------------------------------ */
/* Apply-mode smoke delay                                             */
/* ------------------------------------------------------------------ */

async function applySmokeDelay() {
  if (!opts.apply) {
    logger.info("[DRY RUN] No changes will be made. Use --apply to write.");
    return;
  }
  logger.warn(
    "[APPLY] Writing to target DBs. Have you stopped server.js? " +
      "(Ctrl+C within 5s to abort)",
  );
  await new Promise((r) => setTimeout(r, 5000));
}

/* ------------------------------------------------------------------ */
/* Main                                                              */
/* ------------------------------------------------------------------ */

async function main() {
  await applySmokeDelay();

  const hub = buildHub();
  const branches = buildBranches(opts.branch);
  if (branches.length === 0) {
    logger.error("No branches selected.");
    process.exit(2);
  }

  const tables = opts.tables
    ? opts.tables.filter((t) => ALLOWED_TABLES.has(t))
    : [...ALLOWED_TABLES];

  if (opts.tables) {
    const bad = opts.tables.filter((t) => !ALLOWED_TABLES.has(t));
    if (bad.length) {
      logger.error(`Unknown table(s): ${bad.join(", ")}`);
      process.exit(2);
    }
  }

  const pkFileSet = opts.pkFile ? loadPkFile(opts.pkFile) : null;

  let sinceDate = null;
  if (opts.since) {
    const d = new Date(opts.since);
    if (Number.isNaN(d.getTime())) {
      logger.error(`Invalid --since: ${opts.since}`);
      process.exit(2);
    }
    sinceDate = d;
  }

  let anyFailure = false;
  const summary = [];

  for (const branch of branches) {
    logger.info(`=== Branch [${branch.id}] direction=${opts.direction} ===`);

    const directions =
      opts.direction === "both"
        ? [
            { label: "hub->branch", src: hub, tgt: branch },
            { label: "branch->hub", src: branch, tgt: hub },
          ]
        : opts.direction === "hub-to-branch"
          ? [{ label: "hub->branch", src: hub, tgt: branch }]
          : [{ label: "branch->hub", src: branch, tgt: hub }];

    for (const dir of directions) {
      logger.info(`--- ${dir.label} ---`);

      for (const table of tables) {
        try {
          const { missing, srcCols, tgtCols } = await findMissing(
            dir.src,
            dir.tgt,
            table,
            sinceDate,
          );

          let toCopy = missing;
          if (pkFileSet) {
            toCopy = missing.filter((k) => pkFileSet.has(k));
          }

          if (toCopy.length === 0) {
            logger.debug(`  [${table}] nothing to copy`);
            continue;
          }

          if (toCopy.length > opts.maxRows) {
            logger.warn(
              `  [${table}] ${toCopy.length} rows exceeds --max-rows ${opts.maxRows} — SKIPPING`,
            );
            anyFailure = true;
            summary.push({
              branch: branch.id,
              direction: dir.label,
              table,
              count: toCopy.length,
              status: "REFUSED (exceeds max-rows)",
            });
            continue;
          }

          if (dir.label === "branch->hub") {
            const phantom = await phantomStoreCheck(dir.src, dir.tgt, table);
            if (phantom) {
              logger.warn(`  [${table}] ${phantom.message}`);
            }
          }

          logger.info(
            `  [${table}] ${toCopy.length} missing row(s) ` +
              `(src PK cols: ${srcCols.join(",")}, tgt PK cols: ${tgtCols.join(",")})`,
          );

          if (!opts.apply) {
            summary.push({
              branch: branch.id,
              direction: dir.label,
              table,
              count: toCopy.length,
              status: "dry-run",
            });
            continue;
          }

          const result = await copyRows(
            dir.src,
            dir.tgt,
            table,
            toCopy,
            tgtCols,
          );
          logger.info(
            `  [${table}] copied=${result.copied} failed=${result.failed}`,
          );
          if (result.failed > 0) {
            anyFailure = true;
            for (const e of result.errors.slice(0, 5)) {
              logger.error(`    chunk ${e.chunk}: ${e.error}`);
            }
          }
          summary.push({
            branch: branch.id,
            direction: dir.label,
            table,
            attempted: toCopy.length,
            copied: result.copied,
            failed: result.failed,
            status: result.failed === 0 ? "ok" : "partial",
          });
        } catch (err) {
          anyFailure = true;
          logger.error(`  [${table}] error: ${err.message}`);
          summary.push({
            branch: branch.id,
            direction: dir.label,
            table,
            error: err.message,
            status: "error",
          });
        }
      }
    }
  }

  console.log("\n=== SUMMARY ===");
  if (summary.length === 0) {
    console.log("Nothing to copy (all tables in parity).");
  } else {
    console.table(
      summary.map((s) => ({
        branch: s.branch,
        direction: s.direction,
        table: s.table,
        count: s.count ?? s.attempted,
        copied: s.copied ?? "-",
        failed: s.failed ?? "-",
        status: s.status,
      })),
    );
  }

  await hub.db.destroy().catch(() => {});
  await Promise.allSettled(branches.map((b) => b.db.destroy()));

  process.exit(anyFailure ? 1 : 0);
}

main().catch((err) => {
  logger.error(`[Fatal] ${err.stack || err.message}`);
  process.exit(2);
});
