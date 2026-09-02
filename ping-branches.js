require("dotenv").config();
const knex = require("knex");

const CHECK_TIMEOUT_MS = parseInt(process.env.CHECK_TIMEOUT_MS || "3000", 10);

const BRANCHES_CONFIG = JSON.parse(process.env.BRANCHES_JSON || "[]");

if (BRANCHES_CONFIG.length === 0) {
  console.error("❌ Error: BRANCHES_JSON is empty or not set in .env!");
  process.exit(1);
}

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(label)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

async function checkBranch(branch) {
  const startTime = Date.now();
  let db;

  try {
    db = knex({
      client: branch.client || "pg",
      connection: {
        connectionString: branch.url,
        ssl:
          branch.ssl === true
            ? { rejectUnauthorized: branch.sslInsecure !== true }
            : false,
      },
      pool: { min: 0, max: 1 },
    });

    await withTimeout(db.raw("SELECT 1"), CHECK_TIMEOUT_MS, "TIMEOUT");
    const latency = Date.now() - startTime;

    return {
      Branch: branch.id,
      Client: branch.client || "pg",
      Status: "🟢 ONLINE",
      Latency: `${latency} ms`,
      Detail: "",
    };
  } catch (err) {
    const reason =
      err.message === "TIMEOUT"
        ? `timed out after ${CHECK_TIMEOUT_MS}ms`
        : err.code || err.message || "unknown error";

    return {
      Branch: branch.id,
      Client: branch.client || "pg",
      Status: "🔴 OFFLINE / UNREACHABLE",
      Latency: "N/A",
      Detail: String(reason).slice(0, 80),
    };
  } finally {
    if (db) {
      await withTimeout(db.destroy(), 1000, "DESTROY_TIMEOUT").catch(() => {});
    }
  }
}

async function pingAllBranches() {
  console.log(
    "\n===============================================================",
  );
  console.log(
    ` 📡 TAILSCALE MESH HEALTH CHECK (${BRANCHES_CONFIG.length} BRANCHES)`,
  );
  console.log(
    "===============================================================\n",
  );

  const results = await Promise.all(
    BRANCHES_CONFIG.map((branch) => checkBranch(branch)),
  );

  console.table(results);

  const onlineCount = results.filter((r) => r.Status.includes("ONLINE")).length;
  const offlineCount = results.length - onlineCount;
  console.log(
    `Summary: ${onlineCount}/${BRANCHES_CONFIG.length} Branches Online.`,
  );
  console.log(
    "===============================================================\n",
  );

  process.exitCode = offlineCount > 0 ? 1 : 0;
}

pingAllBranches();
