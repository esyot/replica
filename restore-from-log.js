#!/usr/bin/env node
/**
 * restore-from-log.js — Rebuild rows in any table from sym_change_log.
 *
 * Reads INSERT (or DELETE, for pre-delete state) events from sym_change_log
 * on a source node, reconstructs the rows, and upserts them into one or
 * more target nodes.
 *
 * Use cases:
 *   - Recover rows deleted by an accidental sanitize
 *   - Recover a table after a bad schema change
 *   - Seed a new node from existing history
 *   - Recover from the sym_change_log after a physical table loss
 *
 * Usage:
 *   node restore-from-log.js --tables=pos_session --from=hub --to=hub
 *   node restore-from-log.js --tables=pos_session,cash_count \
 *     --from=hub --to=hub,NODE_BRANCH_02,NODE_BRANCH_03
 *   node restore-from-log.js --tables=pos_session --from=hub --to=hub \
 *     --operation=DELETE --since=2026-11-11
 *   node restore-from-log.js --tables=pos_session --from=hub --to=hub --dry-run
 *
 * Flags:
 *   --tables=T1,T2          tables to recover (required)
 *   --from=ID               source node with sym_change_log (required)
 *   --to=ID1,ID2            target nodes to write to (required; may include source)
 *   --operation=INSERT|DELETE|BOTH   which events to replay (default: INSERT)
 *   --since=YYYY-MM-DD      only events with created_at >= this (optional)
 *   --until=YYYY-MM-DD      only events with created_at <= this (optional)
 *   --record-ids=ID1,ID2    only events for these primary_key_val (optional)
 *   --batch-size=N          upsert chunk size (default 200)
 *   --dry-run               show plan, no writes
 *   --apply                 execute (mutually exclusive with --dry-run)
 *
 * Notes:
 *   - INSERT events replay the exact row as it existed when written.
 *   - DELETE events carry the row state *just before* deletion — useful
 *     for recovering rows that were accidentally deleted.
 *   - When a table has both INSERTs and DELETEs for the same PK, the
 *     latest event wins (sorted by change_id). This reconstructs the
 *     current logical state at the time of the last event.
 *   - Idempotent: existing rows are updated to match the reconstructed
 *     state. If a PK has a later DELETE event and no re-INSERT, the row
 *     is removed from the target.
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

const opts = {
  tables: [],
  fromId: null,
  toIds: [],
  operation: "INSERT", // INSERT | DELETE | BOTH
  since: null,
  until: null,
  recordIds: null,
  batchSize: 200,
  dryRun: true,
};

for (const a of process.argv.slice(2)) {
  if (a === "--apply") opts.dryRun = false;
  else if (a === "--dry-run") opts.dryRun = true;
  else if (a === "--help" || a === "-h") {
    console.log(`
Usage: node restore-from-log.js --tables=... --from=... --to=... [options]

  --tables=T1,T2          tables to recover (required)
  --from=ID               source node (must have sym_change_log) (required)
  --to=ID1,ID2            target nodes to write to (required)
  --operation=INSERT      INSERT (default) | DELETE | BOTH
  --since=YYYY-MM-DD      only events on/after this date
  --until=YYYY-MM-DD      only events on/before this date
  --record-ids=ID1,ID2    only these primary_key_val values
  --batch-size=N          upsert chunk size (default 200)
  --apply                 execute writes
  --dry-run               show plan only (default)

Examples:
  # Recover pos_session from hub's sym_change_log into all nodes
  node restore-from-log.js --tables=pos_session --from=hub --to=hub,NODE_BRANCH_02,NODE_BRANCH_03 --apply

  # Preview what would be restored (same command, no --apply)
  node restore-from-log.js --tables=pos_session --from=hub --to=hub

  # Recover only DELETE events (rows that were removed) from a specific day
  node restore-from-log.js --tables=pos_session,cash_count --from=hub --to=hub --operation=DELETE --since=2026-11-11 --apply

  # Recover specific rows by PK
  node restore-from-log.js --tables=pos_session --from=hub --to=hub --record-ids=29880cee-...,8f5eb9d1-... --apply
`);
    process.exit(0);
  } else if (a.startsWith("--tables=")) {
    opts.tables = a
      .slice(9)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  } else if (a.startsWith("--from=")) {
    opts.fromId = a.slice(7);
  } else if (a.startsWith("--to=")) {
    opts.toIds = a
      .slice(5)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  } else if (a.startsWith("--operation=")) {
    const v = a.slice(12).toUpperCase();
    if (!["INSERT", "DELETE", "BOTH"].includes(v)) {
      logger.error(`--operation must be INSERT|DELETE|BOTH, got "${v}"`);
      process.exit(2);
    }
    opts.operation = v;
  } else if (a.startsWith("--since=")) {
    opts.since = a.slice(8);
  } else if (a.startsWith("--until=")) {
    opts.until = a.slice(8);
  } else if (a.startsWith("--record-ids=")) {
    opts.recordIds = a
      .slice(13)
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
if (!opts.fromId) {
  logger.error("--from is required");
  process.exit(2);
}
if (opts.toIds.length === 0) {
  logger.error("--to is required");
  process.exit(2);
}

if (opts.dryRun) {
  logger.info(
    "==================================================================\n" +
      "  DRY RUN — no writes. Pass --apply to execute.\n" +
      "==================================================================",
  );
}

/* ------------------------------------------------------------------ */
/* Node resolution                                                    */
/* ------------------------------------------------------------------ */

function normalizeClient(c) {
  if (!c) return "mysql2";
  const cc = c.toLowerCase().trim();
  if (cc === "mysql") return "mysql2";
  if (cc === "postgresql") return "pg";
  return cc;
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
    byId.set(hubId, {
      id: hubId,
      client: normalizeClient(process.env.LOCAL_CLIENT),
      url: process.env.LOCAL_URL,
      isHub: true,
    });
    byId.set("hub", byId.get(hubId));
  }
  let branches = [];
  try {
    branches = JSON.parse(process.env.BRANCHES_JSON || "[]");
  } catch (err) {
    logger.error(`Invalid BRANCHES_JSON: ${err.message}`);
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

/* ------------------------------------------------------------------ */
/* Event reading                                                      */
/* ------------------------------------------------------------------ */

async function readEvents(db, table, opFilter) {
  let q = db("sym_change_log")
    .select(
      "change_id",
      "operation",
      "primary_key_val",
      "row_data",
      "created_at",
    )
    .where("table_name", table)
    .orderBy("change_id", "asc");

  if (opFilter.length === 1) {
    q = q.where("operation", opFilter[0]);
  } else if (opFilter.length > 1) {
    q = q.whereIn("operation", opFilter);
  }

  if (opts.since) q = q.where("created_at", ">=", opts.since);
  if (opts.until) q = q.where("created_at", "<=", opts.until);

  if (opts.recordIds && opts.recordIds.length > 0) {
    q = q.whereIn("primary_key_val", opts.recordIds);
  }

  return q;
}

/* ------------------------------------------------------------------ */
/* Reconstruction                                                     */
/* ------------------------------------------------------------------ */

/**
 * Given a list of events (ordered by change_id asc), compute the final
 * state for each primary key:
 *   - INSERT/UPDATE: row exists with row_data
 *   - DELETE: row does not exist (removed)
 *
 * Returns a Map: pk → { state: "present"|"absent", row }
 * The `row` is the decoded object of the last write event for that PK.
 * For "absent" entries, `row` holds the pre-delete snapshot for reference.
 */
function reconstructState(events) {
  const byPk = new Map();

  for (const evt of events) {
    const pk = evt.primary_key_val;
    if (!pk) continue;

    let payload = null;
    try {
      payload = JSON.parse(evt.row_data);
    } catch {
      logger.warn(
        `change_id=${evt.change_id} (${evt.operation}): row_data is not valid JSON — skipping`,
      );
      continue;
    }
    if (!payload || typeof payload !== "object") continue;

    if (evt.operation === "DELETE") {
      byPk.set(pk, {
        state: "absent",
        row: payload,
        lastEventId: evt.change_id,
      });
    } else {
      // INSERT or UPDATE
      byPk.set(pk, {
        state: "present",
        row: payload,
        lastEventId: evt.change_id,
      });
    }
  }

  return byPk;
}

/* ------------------------------------------------------------------ */
/* Apply                                                              */
/* ------------------------------------------------------------------ */

async function chunked(rows, size, fn) {
  for (let i = 0; i < rows.length; i += size) {
    await fn(rows.slice(i, i + size));
  }
}

async function applyToTarget(target, table, pkCols, reconstructed) {
  const db = target.db;

  const present = [];
  const absent = [];
  for (const [pk, entry] of reconstructed) {
    if (entry.state === "present") present.push(entry.row);
    else absent.push(entry.row);
  }

  if (opts.dryRun) {
    logger.info(
      `  [${target.id}] ${table}: would upsert ${present.length}, would delete ${absent.length}`,
    );
    const sample = present.slice(0, 3).map((r) => r[pkCols[0]]);
    if (sample.length > 0) {
      logger.info(
        `    upsert sample: ${sample.join(", ")}${present.length > 3 ? ", ..." : ""}`,
      );
    }
    return;
  }

  const trxResult = { upserted: 0, deleted: 0 };

  await db.transaction(async (trx) => {
    await trx.raw("SET FOREIGN_KEY_CHECKS = 0");
    try {
      // Deletes first (rows whose latest event was DELETE)
      if (absent.length > 0) {
        await chunked(absent, opts.batchSize, async (chunk) => {
          if (pkCols.length === 1) {
            const col = pkCols[0];
            await trx(table)
              .whereIn(
                col,
                chunk.map((r) => r[col]),
              )
              .del();
          } else {
            await trx(table)
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
        trxResult.deleted = absent.length;
      }

      // Upserts
      if (present.length > 0) {
        await chunked(present, opts.batchSize, async (chunk) => {
          await trx(table).insert(chunk).onConflict(pkCols).merge();
        });
        trxResult.upserted = present.length;
      }
    } finally {
      await trx.raw("SET FOREIGN_KEY_CHECKS = 1");
    }
  });

  logger.info(
    `  [${target.id}] ${table}: upserted ${trxResult.upserted}, deleted ${trxResult.deleted}`,
  );
}

/* ------------------------------------------------------------------ */
/* Main                                                               */
/* ------------------------------------------------------------------ */

async function main() {
  const nodes = resolveNodes();
  const source = nodes.get(opts.fromId);
  if (!source) {
    logger.error(
      `--from="${opts.fromId}" not found. Known: ${[...nodes.keys()]
        .filter((k) => k !== "hub")
        .join(", ")}`,
    );
    process.exit(2);
  }

  const targets = [];
  for (const id of opts.toIds) {
    const t = nodes.get(id);
    if (!t) {
      logger.error(`--to entry "${id}" not found`);
      process.exit(2);
    }
    targets.push(t);
  }

  // Open all connections
  source.db = buildKnex(source.client, source.url);
  for (const t of targets) {
    if (!t.db) t.db = buildKnex(t.client, t.url);
  }

  const opFilter =
    opts.operation === "BOTH"
      ? ["INSERT", "UPDATE", "DELETE"]
      : [opts.operation];

  logger.info(
    `Source: [${source.id}]  Operation: ${opFilter.join("|")}  ` +
      `Since: ${opts.since || "any"}  Until: ${opts.until || "any"}`,
  );
  logger.info(`Targets: ${targets.map((t) => `[${t.id}]`).join(", ")}`);
  logger.info("");

  let anyError = false;

  try {
    for (const table of opts.tables) {
      logger.info(`--- ${table} ---`);

      // Read PK columns from target (they should match the source)
      const pkCols = await getPkColumns(
        targets[0].db,
        table,
        targets[0].client,
      );
      if (!pkCols || pkCols.length === 0) {
        logger.error(`  No primary key for ${table} — skipping`);
        anyError = true;
        continue;
      }
      logger.info(`  PK: ${pkCols.join(", ")}`);

      // Read events from source
      let events;
      try {
        events = await readEvents(source.db, table, opFilter);
      } catch (err) {
        logger.error(`  Failed to read source log: ${err.message}`);
        anyError = true;
        continue;
      }

      if (events.length === 0) {
        logger.info(`  No matching events in source log`);
        continue;
      }

      logger.info(`  Read ${events.length} event(s) from source log`);

      // Reconstruct final state per PK
      const reconstructed = reconstructState(events);
      const presentCount = [...reconstructed.values()].filter(
        (e) => e.state === "present",
      ).length;
      const absentCount = reconstructed.size - presentCount;

      logger.info(
        `  Reconstructed ${reconstructed.size} PK(s): ` +
          `${presentCount} present, ${absentCount} absent`,
      );

      // Apply to each target
      for (const target of targets) {
        try {
          await applyToTarget(target, table, pkCols, reconstructed);
        } catch (err) {
          logger.error(`  [${target.id}] apply failed: ${err.message}`);
          anyError = true;
        }
      }
    }
  } finally {
    await source.db.destroy().catch(() => {});
    for (const t of targets) {
      if (t.db) await t.db.destroy().catch(() => {});
    }
  }

  logger.info("");
  if (anyError) {
    logger.error("Result: errors occurred — see above.");
    return 2;
  }
  if (opts.dryRun) {
    logger.info("Result: dry-run complete. Re-run with --apply to execute.");
    return 0;
  }
  logger.info("Result: restore complete.");
  return 0;
}

main()
  .then((c) => process.exit(c))
  .catch((err) => {
    logger.error(`[Fatal] ${err.stack || err.message}`);
    process.exit(2);
  });
