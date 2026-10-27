#!/usr/bin/env node
/**
 * node-copy.js — copy one node's entire database into another node.
 *
 * "Node" = an entry from BRANCHES_JSON, or the hub (LOCAL_*). The tool
 * connects to both via the URLs in your env, uses mysqldump piped into mysql
 * over the network, and never touches sym_change_log / sym_checkpoint /
 * sym_quarantine contents (unless you tell it to).
 *
 * Usage:
 *   node node-copy.js --from=NODE_BRANCH_02 --to=NODE_MAIN_HUB --dry-run
 *   node node-copy.js --from=NODE_BRANCH_02 --to=NODE_MAIN_HUB --apply
 *   node node-copy.js --from=NODE_BRANCH_02 --to=NODE_MAIN_HUB --apply --skip-triggers
 *   node node-copy.js --from=NODE_BRANCH_02 --to=NODE_MAIN_HUB --apply --tables=repa_deposit,repa_group
 *   node node-copy.js --from=NODE_BRANCH_02 --to=NODE_MAIN_HUB --apply --keep-log
 *   node node-copy.js --from=NODE_BRANCH_02 --to=all --apply
 *       -> copies to every other node, asks which source interactively if
 *          --from is not given
 *   node node-copy.js --to=all
 *       -> interactive prompt: pick a source, then copy to all others
 *
 * Required:
 *   mysqldump and mysql must be on PATH (both source and target reachable
 *   from this machine; credentials come from the node URLs).
 *
 * Env:
 *   Same as server.js: LOCAL_*, BRANCHES_JSON, ALLOWED_TABLES
 *
 * Exit codes:
 *   0 = copy succeeded, triggers rebuilt, logs cleared
 *   1 = partial failure (see output)
 *   2 = fatal error / refused
 */

require("dotenv").config();
const knex = require("knex");
const winston = require("winston");
const { spawn } = require("child_process");
const { URL } = require("url");
const fs = require("fs");
const readline = require("readline");

const { setupNodeTriggers, ALLOWED_TABLES } = require("./triggers");

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
  from: null,
  to: null,
  apply: false,
  skipTriggers: false,
  keepLog: false,
  keepCheckpoint: false,
  tables: null,
  backup: true,
};

for (const a of argv) {
  if (a === "--apply") opts.apply = true;
  else if (a === "--dry-run") opts.apply = false;
  else if (a === "--skip-triggers") opts.skipTriggers = true;
  else if (a === "--keep-log") opts.keepLog = true;
  else if (a === "--keep-checkpoint") opts.keepCheckpoint = true;
  else if (a === "--no-backup") opts.backup = false;
  else if (a.startsWith("--from=")) opts.from = a.slice(7);
  else if (a.startsWith("--to=")) opts.to = a.slice(5);
  else if (a.startsWith("--tables="))
    opts.tables = a
      .slice(9)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  else {
    logger.error(`Unknown arg: ${a}`);
    process.exit(2);
  }
}

if (!opts.to) {
  logger.error(
    "--to is required (a node ID, or 'all' to copy to every other node)",
  );
  process.exit(2);
}

if (!opts.apply) {
  logger.warn(
    "==================================================================\n" +
      "  DRY RUN MODE — no writes will happen.\n" +
      "  Re-run with --apply to actually copy.\n" +
      "==================================================================",
  );
} else {
  logger.warn(
    "==================================================================\n" +
      "  APPLY MODE — this will WIPE the target node(s)' data tables and\n" +
      "  overwrite them with a copy of the source node's data.\n" +
      "  Ctrl+C within 10 seconds to abort.\n" +
      "==================================================================",
  );
}

/* ------------------------------------------------------------------ */
/* Env / node resolution                                              */
/* ------------------------------------------------------------------ */

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
  const cfg = JSON.parse(process.env.BRANCHES_JSON || "[]");
  for (const b of cfg) {
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

function buildMysqlEnv(conn) {
  const env = { ...process.env };
  if (conn.password) env.MYSQL_PWD = conn.password;
  return env;
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
    pool: { min: 0, max: 2 },
  });
}

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
  logger.info("");

  while (true) {
    const answer = await prompt(
      `Which node should be the SOURCE (1-${nodes.length})? `,
    );
    const idx = parseInt(answer, 10);
    if (!Number.isNaN(idx) && idx >= 1 && idx <= nodes.length) {
      return nodes[idx - 1];
    }
    logger.warn(
      `Invalid choice "${answer}". Enter a number 1-${nodes.length}.`,
    );
  }
}

async function confirmApply(targets) {
  logger.info("");
  logger.info("About to WIPE data tables on:");
  targets.forEach((t) => logger.info(`  - ${t.id}`));
  logger.info("");
  logger.info("Are you SURE? This is destructive and irreversible without");
  logger.info("the backup file that will be created.");
  const answer = await prompt(
    `Type "yes" to proceed, anything else to abort: `,
  );
  return answer.toLowerCase() === "yes";
}

/* ------------------------------------------------------------------ */
/* Main                                                               */
/* ------------------------------------------------------------------ */

async function main() {
  const nodes = resolveNodes();
  if (nodes.length === 0) {
    logger.error("No nodes configured (LOCAL_URL / BRANCHES_JSON missing)");
    process.exit(2);
  }

  // Resolve source: either from --from, or interactively if --to=all
  let source;
  if (opts.from) {
    source = nodes.find((n) => n.id === opts.from);
    if (!source) {
      logger.error(
        `Source node "${opts.from}" not found. Known nodes: ${nodes
          .map((n) => n.id)
          .join(", ")}`,
      );
      process.exit(2);
    }
  } else if (opts.to === "all") {
    // Interactive source selection only allowed with --to=all
    logger.warn(
      "--from not provided with --to=all — asking interactively which node is the source",
    );
    source = await pickSourceInteractively(nodes);
    logger.info(`Selected source: ${source.id}`);
  } else {
    logger.error(
      "--from is required when --to is a specific node (use --to=all to get an interactive prompt)",
    );
    process.exit(2);
  }

  // Resolve targets
  let targets;
  if (opts.to === "all") {
    targets = nodes.filter((n) => n.id !== source.id);
    if (targets.length === 0) {
      logger.error("--to=all but no other nodes configured");
      process.exit(2);
    }
    logger.info(
      `Copying to ALL other nodes: ${targets.map((t) => t.id).join(", ")}`,
    );
  } else {
    const single = nodes.find((n) => n.id === opts.to);
    if (!single) {
      logger.error(
        `Target node "${opts.to}" not found. Known nodes: ${nodes
          .map((n) => n.id)
          .join(", ")}`,
      );
      process.exit(2);
    }
    if (single.id === source.id) {
      logger.error("Source and target must be different nodes");
      process.exit(2);
    }
    targets = [single];
  }

  if (isPg(source.client) || targets.some((t) => isPg(t.client))) {
    logger.error(
      "This tool only supports MySQL/MariaDB. Postgres users: use pg_dump/psql manually.",
    );
    process.exit(2);
  }

  // Which tables?
  let tables = opts.tables
    ? opts.tables.filter((t) => ALLOWED_TABLES.has(t))
    : [...ALLOWED_TABLES];

  if (tables.length === 0) {
    logger.error("No tables to copy (check --tables / ALLOWED_TABLES)");
    process.exit(2);
  }

  // Dry-run: print plan, exit
  if (!opts.apply) {
    logger.info("");
    logger.info(`Source:  ${source.id}`);
    logger.info(`Targets: ${targets.map((t) => t.id).join(", ")}`);
    logger.info(`Tables (${tables.length}): ${tables.join(", ")}`);
    logger.info("");
    logger.info("[DRY RUN] Would do, per target:");
    logger.info(
      "  1. Back up target's data tables -> backup-<target>-<ts>.sql",
    );
    logger.info("  2. Drop sym triggers on target");
    logger.info("  3. Truncate target's data tables");
    logger.info("  4. mysqldump source | mysql target (data only)");
    logger.info("  5. Rebuild sym triggers on target");
    logger.info("  6. TRUNCATE sym_change_log on source + target");
    logger.info("  7. TRUNCATE sym_checkpoint on target");
    logger.info("");
    logger.info("Run with --apply to execute.");
    return 0;
  }

  // Confirm before destructive apply
  const confirmed = await confirmApply(targets);
  if (!confirmed) {
    logger.warn("Aborted by user.");
    return 2;
  }

  // 10-second abort window after interactive confirmation too
  logger.warn("Starting in 10 seconds. Ctrl+C to abort...");
  await new Promise((r) => setTimeout(r, 10000));

  // Run the copy per target
  for (const target of targets) {
    logger.info(`\n========== Copy ${source.id} -> ${target.id} ==========`);
    const ok = await copyNode(source, target, tables);
    if (!ok) {
      logger.error(
        `Copy to [${target.id}] failed; continuing with next target`,
      );
    }
  }

  logger.info("\n=== All targets processed ===");
  logger.info("Restart server.js on all nodes to pick up the new state.");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Copy one node to another                                           */
/* ------------------------------------------------------------------ */

async function copyNode(source, target, tables) {
  const srcConn = parseMysqlUrl(source.url);
  const tgtConn = parseMysqlUrl(target.url);

  logger.info(
    `Source: [${source.id}] ${srcConn.host}:${srcConn.port}/${srcConn.database}`,
  );
  logger.info(
    `Target: [${target.id}] ${tgtConn.host}:${tgtConn.port}/${tgtConn.database}`,
  );

  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const backupFile = `backup-${target.id}-${ts}.sql`;

  /* ------------------------------------------------------------ */
  /* Phase 1: back up target                                       */
  /* ------------------------------------------------------------ */
  if (opts.backup) {
    logger.info(`[1/7] Backing up [${target.id}] data tables -> ${backupFile}`);
    const dumpArgs = [
      "-h",
      tgtConn.host,
      "-P",
      String(tgtConn.port),
      "-u",
      tgtConn.user,
      "--single-transaction",
      "--routines=false",
      "--triggers=false",
      tgtConn.database,
      ...tables,
    ];
    try {
      const out = fs.createWriteStream(backupFile);
      const dump = spawn("mysqldump", dumpArgs, {
        env: buildMysqlEnv(tgtConn),
        stdio: ["ignore", "pipe", "inherit"],
      });
      dump.stdout.pipe(out);
      const code = await new Promise((resolve, reject) => {
        dump.on("close", resolve);
        dump.on("error", reject);
      });
      if (code !== 0) {
        throw new Error(`mysqldump exit ${code}`);
      }
      const stat = fs.statSync(backupFile);
      logger.info(
        `[1/7] Backup written: ${backupFile} (${(stat.size / 1024 / 1024).toFixed(1)} MB)`,
      );
    } catch (err) {
      logger.error(`Backup failed: ${err.message}`);
      return false;
    }
  } else {
    logger.warn("[1/7] Skipping backup (--no-backup)");
  }

  /* ------------------------------------------------------------ */
  /* Phase 2: drop sym triggers on target                          */
  /* ------------------------------------------------------------ */
  if (opts.skipTriggers) {
    logger.warn("[2/7] Skipping trigger drop (--skip-triggers)");
  } else {
    logger.info(`[2/7] Dropping sym triggers on [${target.id}]`);
    try {
      const targetDb = buildKnexFor(tgtConn);
      const trigs = await targetDb.raw(
        `SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
         WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME LIKE 'sym_trig_%'`,
      );
      const list = trigs[0] || trigs;
      for (const t of list) {
        await targetDb.raw(`DROP TRIGGER IF EXISTS \`${t.TRIGGER_NAME}\``);
      }
      logger.info(`[2/7] Dropped ${list.length} trigger(s)`);
      await targetDb.destroy();
    } catch (err) {
      logger.error(`Trigger drop failed: ${err.message}`);
      return false;
    }
  }

  /* ------------------------------------------------------------ */
  /* Phase 3: truncate target tables (FK off)                      */
  /* ------------------------------------------------------------ */
  logger.info(`[3/7] Truncating ${tables.length} tables on [${target.id}]`);
  try {
    const targetDb = buildKnexFor(tgtConn);
    await targetDb.raw("SET FOREIGN_KEY_CHECKS = 0");
    let truncated = 0;
    for (const tbl of tables) {
      try {
        await targetDb.raw(`TRUNCATE TABLE \`${tbl}\``);
        truncated++;
      } catch (err) {
        logger.warn(`  truncate ${tbl}: ${err.message}`);
      }
    }
    await targetDb.raw("SET FOREIGN_KEY_CHECKS = 1");
    await targetDb.destroy();
    logger.info(`[3/7] Truncated ${truncated}/${tables.length} tables`);
  } catch (err) {
    logger.error(`Truncate phase failed: ${err.message}`);
    return false;
  }

  /* ------------------------------------------------------------ */
  /* Phase 4: dump source, pipe into target                        */
  /* ------------------------------------------------------------ */
  logger.info(`[4/7] Copying data: ${source.id} -> ${target.id}`);
  try {
    const dumpArgs = [
      "-h",
      srcConn.host,
      "-P",
      String(srcConn.port),
      "-u",
      srcConn.user,
      "--single-transaction",
      "--no-create-info",
      "--routines=false",
      "--triggers=false",
      "--extended-insert",
      "--complete-insert",
      srcConn.database,
      ...tables,
    ];
    const loadArgs = [
      "-h",
      tgtConn.host,
      "-P",
      String(tgtConn.port),
      "-u",
      tgtConn.user,
      tgtConn.database,
    ];

    const dumpProc = spawn("mysqldump", dumpArgs, {
      env: buildMysqlEnv(srcConn),
      stdio: ["ignore", "pipe", "inherit"],
    });
    const loadProc = spawn("mysql", loadArgs, {
      env: buildMysqlEnv(tgtConn),
      stdio: ["pipe", "inherit", "inherit"],
    });

    dumpProc.stdout.pipe(loadProc.stdin);

    const [dumpCode, loadCode] = await Promise.all([
      new Promise((resolve, reject) => {
        dumpProc.on("close", resolve);
        dumpProc.on("error", reject);
      }),
      new Promise((resolve, reject) => {
        loadProc.on("close", resolve);
        loadProc.on("error", reject);
      }),
    ]);

    if (dumpCode !== 0) throw new Error(`mysqldump exit ${dumpCode}`);
    if (loadCode !== 0) throw new Error(`mysql exit ${loadCode}`);

    logger.info(`[4/7] Data copy complete`);
  } catch (err) {
    logger.error(`Data copy failed: ${err.message}`);
    logger.error(
      `Target may be inconsistent. Restore from ${backupFile} if needed.`,
    );
    return false;
  }

  /* ------------------------------------------------------------ */
  /* Phase 5: rebuild triggers on target                           */
  /* ------------------------------------------------------------ */
  if (opts.skipTriggers) {
    logger.warn("[5/7] Skipping trigger rebuild (--skip-triggers)");
  } else {
    logger.info(`[5/7] Rebuilding sym triggers on [${target.id}]`);
    try {
      const targetDb = buildKnexFor(tgtConn);
      const summary = await setupNodeTriggers({
        id: target.id,
        client: "mysql2",
        db: targetDb,
      });
      logger.info(
        `[5/7] Rebuilt: ok=${summary.ok.length} skipped=${summary.skipped.length} failed=${summary.failed.length}`,
      );
      await targetDb.destroy();
    } catch (err) {
      logger.error(`Trigger rebuild failed: ${err.message}`);
      return false;
    }
  }

  /* ------------------------------------------------------------ */
  /* Phase 6: clear sym_change_log on source + target              */
  /* ------------------------------------------------------------ */
  if (opts.keepLog) {
    logger.warn("[6/7] Skipping sym_change_log truncate (--keep-log)");
  } else {
    logger.info(
      `[6/7] TRUNCATE sym_change_log on [${source.id}] and [${target.id}]`,
    );
    for (const n of [source, target]) {
      try {
        const c = parseMysqlUrl(n.url);
        const db = buildKnexFor(c);
        await db.raw("TRUNCATE TABLE sym_change_log");
        logger.info(`  [${n.id}] sym_change_log cleared`);
        await db.destroy();
      } catch (err) {
        if (!/doesn't exist/i.test(err.message)) {
          logger.warn(`  [${n.id}] clear sym_change_log: ${err.message}`);
        }
      }
    }
  }

  /* ------------------------------------------------------------ */
  /* Phase 7: clear sym_checkpoint on target                       */
  /* ------------------------------------------------------------ */
  if (opts.keepCheckpoint) {
    logger.warn("[7/7] Skipping sym_checkpoint truncate (--keep-checkpoint)");
  } else {
    logger.info(`[7/7] TRUNCATE sym_checkpoint on [${target.id}]`);
    try {
      const c = parseMysqlUrl(target.url);
      const db = buildKnexFor(c);
      await db.raw("TRUNCATE TABLE sym_checkpoint");
      logger.info(`  [${target.id}] sym_checkpoint cleared`);
      await db.destroy();
    } catch (err) {
      if (!/doesn't exist/i.test(err.message)) {
        logger.warn(`  clear sym_checkpoint: ${err.message}`);
      }
    }
  }

  logger.info(`\n[${target.id}] DONE. Backup at: ${backupFile}`);
  return true;
}

/* ------------------------------------------------------------------ */
/* Entry                                                              */
/* ------------------------------------------------------------------ */

main()
  .then((code) => process.exit(code || 0))
  .catch((err) => {
    logger.error(`[Fatal] ${err.stack || err.message}`);
    process.exit(2);
  });
