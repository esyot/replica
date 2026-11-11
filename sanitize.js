#!/usr/bin/env node
/**
 * sanitize.js — Reconcile specific tables across nodes from a source of truth.
 *
 * Usage:
 *   node sanitize.js --tables=pos_session --source_id=hub --targets=branch_2,branch_3 [--apply]
 *
 * Modes:
 *   (default)   dry-run: show what would change
 *   --apply     actually perform the reconciliation
 *
 * What it does:
 *   For each --tables entry:
 *     1. Reads full content of the table from --source_id (the truth).
 *     2. Reads full content of the table from each --targets node.
 *     3. Computes:
 *          - rows to INSERT   (in source, missing on target)
 *          - rows to UPDATE   (in source, present on target, differing)
 *          - rows to DELETE   (on target, missing from source)
 *     4. (With --apply) performs the changes inside a transaction:
 *          DELETE missing, then UPSERT present, in FK-safe order.
 *     5. Reports what it did (or would do).
 *
 * IDs: --source_id and each --targets entry must match either:
 *   - the hub id    (LOCAL_ID, default NODE_MAIN_HUB)
 *   - a branch id   (from BRANCHES_JSON)
 *   - a special alias "hub" that maps to LOCAL_ID
 *
 * Requires the same .env as server.js (LOCAL_URL, BRANCHES_JSON, etc).
 *
 * Exit: 0 success / no drift; 1 drift detected in dry-run;
 *       2 fatal (bad args, missing node, DB error).
 */

require("dotenv").config();
const knex = require("knex");
const winston = require("winston");
const { URL } = require("url");

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
  tables: [],
  sourceId: null,
  targetIds: [],
  apply: false,
  batchSize: 200,
};

for (const a of argv) {
  if (a === "--apply") opts.apply = true;
  else if (a === "--dry-run") opts.apply = false;
  else if (a === "--help" || a === "-h") {
    console.log(`
Usage: node sanitize.js --tables=... --source_id=... --targets=... [--apply]

  --tables=T1,T2        tables to reconcile (must be in ALLOWED_TABLES)
  --source_id=ID        source node id (or "hub")
  --targets=ID1,ID2     target node ids (comma-separated)
  --apply               actually write changes (default: dry-run)
  --batch-size=N        upsert/delete chunk size (default 200)
`);
    process.exit(0);
  } else if (a.startsWith("--tables=")) {
    opts.tables = a
      .slice(9)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  } else if (a.startsWith("--source_id=")) {
    opts.sourceId = a.slice(12);
  } else if (a.startsWith("--targets=")) {
    opts.targetIds = a
      .slice(10)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  } else if (a.startsWith("--batch-size=")) {
    opts.batchSize = parseInt(a.slice(13), 10) || 200;
  } else {
    logger.error(`Unknown arg: ${a}`);
    process.exit(2);
  }
}

if (opts.tables.length === 0) {
  logger.error("--tables is required");
  process.exit(2);
}
if (!opts.sourceId) {
  logger.error("--source_id is required");
  process.exit(2);
}
if (opts.targetIds.length === 0) {
  logger.error("--targets is required");
  process.exit(2);
}

if (!opts.apply) {
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

function normalizeClient(client) {
  if (!client) return "mysql2";
  const c = client.toLowerCase().trim();
  if (c === "mysql") return "mysql2";
  if (c === "postgresql") return "pg";
  return c;
}

function parseConnectionString(urlStr, defaultPort = 3306) {
  if (!urlStr) throw new Error("empty connection string");
  const parsed = new URL(urlStr);
  return {
    host: parsed.hostname || "127.0.0.1",
    port: parsed.port ? parseInt(parsed.port, 10) : defaultPort,
    user: parsed.username ? decodeURIComponent(parsed.username) : undefined,
    password: parsed.password ? decodeURIComponent(parsed.password) : undefined,
    database: parsed.pathname.slice(1),
  };
}

function buildKnex(nodeClient, url) {
  if (nodeClient === "pg" || nodeClient === "postgres") {
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

/* ------------------------------------------------------------------ */
/* Node resolution                                                    */
/* ------------------------------------------------------------------ */

function resolveNodes() {
  const byId = new Map();

  const hubId = process.env.LOCAL_ID || "NODE_MAIN_HUB";
  if (process.env.LOCAL_URL) {
    byId.set(hubId, {
      id: hubId,
      client: normalizeClient(process.env.LOCAL_CLIENT),
      url: process.env.LOCAL_URL,
      isHub: true,
    });
    // alias
    byId.set("hub", byId.get(hubId));
  }

  let branches = [];
  try {
    branches = JSON.parse(process.env.BRANCHES_JSON || "[]");
  } catch (err) {
    logger.error(`Failed to parse BRANCHES_JSON: ${err.message}`);
    process.exit(2);
  }
  for (const b of branches) {
    if (!b.id || !b.url) {
      logger.error(`Branch entry missing id/url: ${JSON.stringify(b)}`);
      process.exit(2);
    }
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
/* PK discovery                                                       */
/* ------------------------------------------------------------------ */

async function getPkColumns(db, tableName, client) {
  if (client === "pg" || client === "postgres") {
    const res = await db.raw(
      `SELECT a.attname
       FROM pg_index i
       JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
       JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
       WHERE i.indrelid = ?::regclass AND i.indisprimary
       ORDER BY k.ord`,
      [tableName],
    );
    return res.rows.map((r) => r.attname);
  }
  const res = await db.raw(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [
    tableName,
  ]);
  const rows = res[0] || res;
  return rows
    .slice()
    .sort((a, b) => Number(a.Seq_in_index) - Number(b.Seq_in_index))
    .map((r) => r.Column_name);
}

function makeKeyGetter(pkCols) {
  if (pkCols.length === 1) {
    const col = pkCols[0];
    return (row) => (row[col] == null ? null : String(row[col]));
  }
  return (row) => JSON.stringify(pkCols.map((c) => row[c] ?? null));
}

function makeKeyFromValues(pkCols, values) {
  if (pkCols.length === 1) {
    const v = values[0];
    return v == null ? null : String(v);
  }
  return JSON.stringify(values.map((v) => (v == null ? null : String(v))));
}

/* ------------------------------------------------------------------ */
/* Diff                                                               */
/* ------------------------------------------------------------------ */

function diffRows(sourceRows, targetRows, pkCols) {
  const keyOf = makeKeyGetter(pkCols);
  const srcMap = new Map();
  for (const r of sourceRows) {
    const k = keyOf(r);
    if (k != null) srcMap.set(k, r);
  }
  const tgtMap = new Map();
  for (const r of targetRows) {
    const k = keyOf(r);
    if (k != null) tgtMap.set(k, r);
  }

  const toInsert = [];
  const toUpdate = [];
  const toDelete = [];

  for (const [k, srcRow] of srcMap) {
    const tgtRow = tgtMap.get(k);
    if (!tgtRow) {
      toInsert.push(srcRow);
      continue;
    }
    // Compare all non-PK columns
    const nonPk = Object.keys(srcRow).filter((c) => !pkCols.includes(c));
    let differs = false;
    for (const c of nonPk) {
      if (String(srcRow[c] ?? "") !== String(tgtRow[c] ?? "")) {
        differs = true;
        break;
      }
    }
    if (differs) toUpdate.push(srcRow);
  }
  for (const [k, tgtRow] of tgtMap) {
    if (!srcMap.has(k)) toDelete.push(tgtRow);
  }

  return { toInsert, toUpdate, toDelete };
}

/* ------------------------------------------------------------------ */
/* Apply                                                              */
/* ------------------------------------------------------------------ */

async function chunked(rows, size, fn) {
  for (let i = 0; i < rows.length; i += size) {
    await fn(rows.slice(i, i + size));
  }
}

async function applyDiff(db, tableName, pkCols, diff, batchSize) {
  const { toInsert, toUpdate, toDelete } = diff;

  // Deletes first (in case of PK reuse), then upserts.
  if (toDelete.length > 0) {
    await chunked(toDelete, batchSize, async (chunk) => {
      if (pkCols.length === 1) {
        const col = pkCols[0];
        await db(tableName)
          .whereIn(
            col,
            chunk.map((r) => r[col]),
          )
          .del();
      } else {
        await db(tableName)
          .where(function () {
            for (const row of chunk) {
              this.orWhere(function () {
                for (const c of pkCols) this.andWhere(c, row[c]);
              });
            }
          })
          .del();
      }
    });
  }

  const upserts = [...toInsert, ...toUpdate];
  if (upserts.length > 0) {
    await chunked(upserts, batchSize, async (chunk) => {
      await db(tableName).insert(chunk).onConflict(pkCols).merge();
    });
  }
}

/* ------------------------------------------------------------------ */
/* Main                                                               */
/* ------------------------------------------------------------------ */

async function main() {
  const nodes = resolveNodes();

  const source = nodes.get(opts.sourceId);
  if (!source) {
    logger.error(
      `--source_id="${opts.sourceId}" not found. Known ids: ${[...nodes.keys()]
        .filter((k) => k !== "hub")
        .join(", ")}`,
    );
    process.exit(2);
  }

  const targets = [];
  for (const id of opts.targetIds) {
    const t = nodes.get(id);
    if (!t) {
      logger.error(`--targets entry "${id}" not found`);
      process.exit(2);
    }
    if (t.id === source.id) {
      logger.warn(`Skipping target "${id}": same as source`);
      continue;
    }
    targets.push(t);
  }
  if (targets.length === 0) {
    logger.error("No valid targets after filtering");
    process.exit(2);
  }

  // De-dup tables
  const tables = [...new Set(opts.tables)];

  logger.info(
    `Sanitizing ${tables.length} table(s): ${tables.join(", ")}\n` +
      `Source: [${source.id}] ${source.url.replace(/\/\/[^@]+@/, "//***@")}\n` +
      `Targets: ${targets.map((t) => `[${t.id}]`).join(", ")}\n` +
      `Mode: ${opts.apply ? "APPLY" : "DRY RUN"}`,
  );

  // Open connections
  const sourceDb = buildKnex(source.client, source.url);
  const targetDbs = new Map();
  for (const t of targets) {
    targetDbs.set(t.id, buildKnex(t.client, t.url));
  }

  let anyDrift = false;
  let anyError = false;

  try {
    for (const table of tables) {
      logger.info(`\n--- ${table} ---`);

      // Get PK columns (assume same schema on source and targets)
      let pkCols;
      try {
        pkCols = await getPkColumns(sourceDb, table, source.client);
      } catch (err) {
        logger.error(`  Cannot read PK columns for ${table}: ${err.message}`);
        anyError = true;
        continue;
      }
      if (!pkCols || pkCols.length === 0) {
        logger.error(`  No primary key for ${table} — skipping`);
        anyError = true;
        continue;
      }

      // Read source
      let sourceRows;
      try {
        sourceRows = await sourceDb.select("*").from(table);
      } catch (err) {
        logger.error(`  Cannot read source: ${err.message}`);
        anyError = true;
        continue;
      }
      logger.info(`  source: ${sourceRows.length} row(s)`);

      for (const target of targets) {
        const targetDb = targetDbs.get(target.id);
        let targetRows;
        try {
          targetRows = await targetDb.select("*").from(table);
        } catch (err) {
          logger.error(`  [${target.id}] Cannot read: ${err.message}`);
          anyError = true;
          continue;
        }

        const diff = diffRows(sourceRows, targetRows, pkCols);
        const total =
          diff.toInsert.length + diff.toUpdate.length + diff.toDelete.length;

        if (total === 0) {
          logger.info(
            `  [${target.id}] in sync (${targetRows.length} rows) ✅`,
          );
          continue;
        }

        anyDrift = true;
        logger.warn(
          `  [${target.id}] drift: +${diff.toInsert.length} ` +
            `~${diff.toUpdate.length} -${diff.toDelete.length} ` +
            `(target has ${targetRows.length})`,
        );

        // Sample of what changes
        const sample = (rows, label) => {
          if (rows.length === 0) return;
          const keyOf = makeKeyGetter(pkCols);
          const keys = rows.slice(0, 5).map(keyOf);
          logger.warn(
            `    ${label}: ${keys.join(", ")}${rows.length > 5 ? ", ..." : ""}`,
          );
        };
        sample(diff.toInsert, "insert");
        sample(diff.toUpdate, "update");
        sample(diff.toDelete, "delete");

        if (opts.apply) {
          try {
            await targetDb.transaction(async (trx) => {
              await trx.raw("SET FOREIGN_KEY_CHECKS = 0");
              try {
                await applyDiff(trx, table, pkCols, diff, opts.batchSize);
              } finally {
                await trx.raw("SET FOREIGN_KEY_CHECKS = 1");
              }
            });
            logger.info(`  [${target.id}] applied ✅`);
          } catch (err) {
            logger.error(`  [${target.id}] apply failed: ${err.message}`);
            anyError = true;
          }
        }
      }
    }
  } finally {
    await sourceDb.destroy().catch(() => {});
    for (const db of targetDbs.values()) {
      await db.destroy().catch(() => {});
    }
  }

  logger.info("");
  if (anyError) {
    logger.error("Result: ERRORS occurred — see above.");
    return 2;
  }
  if (!anyDrift) {
    logger.info("Result: all targets in sync ✅");
    return 0;
  }
  if (opts.apply) {
    logger.info("Result: reconciliation applied.");
    return 0;
  }
  logger.info("Result: drift detected (dry-run). Re-run with --apply.");
  return 1;
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    logger.error(`[Fatal] ${err.stack || err.message}`);
    process.exit(2);
  });
