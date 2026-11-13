#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const mysql = require("mysql2/promise");

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

function parseConn(urlStr) {
  const u = new URL(urlStr);
  return {
    host: u.hostname,
    port: parseInt(u.port || "3306", 10),
    user: decodeURIComponent(u.username),
    password: decodeURIComponent(u.password),
    database: u.pathname.replace(/^\//, ""),
  };
}

const HUB = {
  id: ENV.LOCAL_ID || "NODE_MAIN_HUB",
  cfg: parseConn(ENV.LOCAL_URL),
};
const BRANCHES = JSON.parse(ENV.BRANCHES_JSON || "[]").map((b) => ({
  id: b.id,
  cfg: parseConn(b.url),
}));
const ALL_NODES = [HUB, ...BRANCHES];

const ALLOWED_TABLES = (ENV.ALLOWED_TABLES || "")
  .split(",")
  .map((t) => t.trim())
  .filter(Boolean);
const ALERT_LAG_MS = parseInt(ENV.LOG_FIDELITY_ALERT_LAG_MS || "300000", 10);
const WEBHOOK = ENV.LOG_FIDELITY_WEBHOOK_URL || "";

async function checkNode(node) {
  const conn = await mysql.createConnection(node.cfg);
  const findings = [];
  try {
    // For each synced table: table's UPDATE_TIME vs MAX(sym_change_log.created_at)
    // WHERE table_name = ?. If table is newer by > ALERT_LAG_MS, it's an orphan write.
    for (const table of ALLOWED_TABLES) {
      try {
        const [[t]] = await conn.execute(
          `SELECT UPDATE_TIME AS u FROM information_schema.TABLES
           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1`,
          [table],
        );
        const u = t?.u;
        if (!u) continue;
        const [[l]] = await conn.execute(
          `SELECT MAX(created_at) AS m FROM sym_change_log WHERE table_name = ?`,
          [table],
        );
        const logMax = l?.m ? new Date(l.m) : new Date(0);
        const lag = new Date(u).getTime() - logMax.getTime();
        if (lag > ALERT_LAG_MS)
          findings.push({ table, updateTime: u, logMax, lagMs: lag });
      } catch (err) {
        findings.push({ table, error: err.message });
      }
    }
  } finally {
    await conn.end();
  }
  return findings;
}

async function main() {
  const all = [];
  for (const n of ALL_NODES) {
    try {
      const f = await checkNode(n);
      if (f.length) all.push({ node: n.id, findings: f });
    } catch (err) {
      all.push({ node: n.id, error: err.message });
    }
  }

  if (all.length === 0) {
    console.log(`[${new Date().toISOString()}] OK — all nodes clean`);
    process.exit(0);
  }

  const lines = [];
  for (const { node, findings, error } of all) {
    if (error) {
      lines.push(`NODE ${node}: CONNECT ERROR ${error}`);
      continue;
    }
    lines.push(`NODE ${node}: ${findings.length} table(s) with orphan writes:`);
    for (const f of findings) {
      if (f.error) lines.push(`  ${f.table}: error ${f.error}`);
      else
        lines.push(
          `  ${f.table}: table updated ${f.updateTime}, log latest ${f.logMax.toISOString()}, lag ${Math.round(f.lagMs / 1000)}s`,
        );
    }
  }

  const report = `[${new Date().toISOString()}] ORPHAN WRITE DETECTED\n${lines.join("\n")}`;
  console.error(report);

  if (WEBHOOK) {
    try {
      await fetch(WEBHOOK, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ text: report, findings: all }),
      });
    } catch (err) {
      console.error(`Webhook failed: ${err.message}`);
    }
  }

  process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(2);
});
