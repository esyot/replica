require("dotenv").config();
const { setupNodeTriggers } = require("./triggers");
const knex = require("knex");

function parseConnectionString(urlStr) {
  if (!urlStr) throw new Error("empty connection string");
  const parsed = new URL(urlStr);
  return {
    host: parsed.hostname || "127.0.0.1",
    port: parsed.port ? parseInt(parsed.port, 10) : 3306,
    user: parsed.username ? decodeURIComponent(parsed.username) : undefined,
    password: parsed.password ? decodeURIComponent(parsed.password) : undefined,
    database: parsed.pathname.replace("/", ""),
  };
}

function normalizeClient(client) {
  if (!client) return "mysql2";
  const c = client.toLowerCase().trim();
  return c === "mysql" ? "mysql2" : c;
}

async function run() {
  const branches = JSON.parse(process.env.BRANCHES_JSON || "[]");

  for (const b of branches) {
    const clientName = normalizeClient(b.client);
    const isPg = clientName === "pg" || clientName === "postgres";
    const db = knex({
      client: clientName,
      connection: isPg
        ? { connectionString: b.url }
        : parseConnectionString(b.url),
    });

    const node = { id: b.id, client: clientName, db };
    console.log(`Setting up triggers for ${node.id}...`);
    await setupNodeTriggers(node);
    await db.destroy();
  }
  console.log("Trigger setup complete.");
}

run().catch((err) => {
  console.error("Trigger setup failed:", err);
  process.exit(1);
});
