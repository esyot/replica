require("dotenv").config();
const knex = require("knex");

// Parse ALLOWED_TABLES dynamically from environment variable
const rawAllowedTables = process.env.ALLOWED_TABLES || "";
const ALLOWED_TABLES = new Set(
  rawAllowedTables
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean),
);

// Cache PK queries in memory per table to reduce DB schema lookup overhead
const pkCache = new Map();

async function getTablePkColumns(node, tableName) {
  const cacheKey = `${node.id}:${tableName}`;
  if (pkCache.has(cacheKey)) {
    return pkCache.get(cacheKey);
  }

  const db = node.db;
  const client = node.client;
  let pkColumns = [];

  if (client === "pg" || client === "postgres") {
    const res = await db.raw(
      `SELECT a.attname
       FROM pg_index i
       JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
       WHERE i.indrelid = ?::regclass AND i.indisprimary;`,
      [tableName],
    );
    pkColumns = res.rows.map((r) => r.attname);
  } else {
    const res = await db.raw(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [
      tableName,
    ]);
    const rows = res[0] || res;
    pkColumns = rows.map((r) => r.Column_name);
  }

  if (pkColumns.length > 0) {
    pkCache.set(cacheKey, pkColumns);
  }

  return pkColumns;
}

async function setupNodeTriggers(node) {
  console.log(
    `Setting up triggers on node: ${node.id} for ${ALLOWED_TABLES.size} table(s)`,
  );
  // Add node database trigger generation/installation logic here if needed
}

module.exports = {
  getTablePkColumns,
  setupNodeTriggers,
  ALLOWED_TABLES,
};

// ONLY EXECUTE SCRIPT IF RUN DIRECTLY (e.g. `node triggers.js`)
if (require.main === module) {
  function parseConnectionString(urlStr) {
    if (!urlStr) throw new Error("empty connection string");
    const parsed = new URL(urlStr);
    return {
      host: parsed.hostname || "127.0.0.1",
      port: parsed.port ? parseInt(parsed.port, 10) : 3306,
      user: parsed.username ? decodeURIComponent(parsed.username) : undefined,
      password: parsed.password
        ? decodeURIComponent(parsed.password)
        : undefined,
      database: parsed.pathname.replace("/", ""),
    };
  }

  function normalizeClient(client) {
    if (!client) return "mysql2";
    const c = client.toLowerCase().trim();
    return c === "mysql" ? "mysql2" : c;
  }

  async function run() {
    const nodesToSetup = [];

    // Load Main Hub
    if (process.env.LOCAL_URL) {
      const hubClient = normalizeClient(process.env.LOCAL_CLIENT);
      nodesToSetup.push({
        id: process.env.LOCAL_ID || "NODE_MAIN_HUB",
        client: hubClient,
        url: process.env.LOCAL_URL,
        ssl: process.env.LOCAL_SSL === "true",
        sslStrict: process.env.LOCAL_SSL_STRICT !== "false",
      });
    }

    // Load Branches
    let branches = [];
    try {
      branches = JSON.parse(process.env.BRANCHES_JSON || "[]");
    } catch (err) {
      console.error("Failed to parse BRANCHES_JSON:", err.message);
    }

    for (const b of branches) {
      const branchClient = normalizeClient(b.client);
      nodesToSetup.push({
        id: b.id,
        client: branchClient,
        url: b.url,
        ssl: b.sslInsecure !== true,
        sslStrict: true,
      });
    }

    if (nodesToSetup.length === 0) {
      console.warn("No database connection strings found in .env.");
      return;
    }

    for (const n of nodesToSetup) {
      const isPg = n.client === "pg" || n.client === "postgres";
      const db = knex({
        client: n.client,
        connection: isPg
          ? {
              connectionString: n.url,
              ssl: n.ssl ? { rejectUnauthorized: n.sslStrict } : false,
            }
          : parseConnectionString(n.url),
      });

      const node = { id: n.id, client: n.client, db };
      console.log(`Setting up triggers for node [${node.id}]...`);
      try {
        await setupNodeTriggers(node);
        console.log(`Successfully configured triggers for [${node.id}]`);
      } catch (err) {
        console.error(
          `Failed to set up triggers for [${node.id}]:`,
          err.message,
        );
      } finally {
        await db.destroy();
      }
    }

    console.log("\nAll trigger setups completed.");
  }

  run().catch((err) => {
    console.error("Fatal error during trigger setup:", err);
    process.exit(1);
  });
}
