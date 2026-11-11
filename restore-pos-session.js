require("dotenv").config();
const knex = require("knex");
const fs = require("fs");
const { URL } = require("url");

function parseConn(url) {
  const u = new URL(url);
  return {
    host: u.hostname,
    port: u.port ? parseInt(u.port, 10) : 3306,
    user: decodeURIComponent(u.username),
    password: decodeURIComponent(u.password),
    database: u.pathname.slice(1),
  };
}

async function main() {
  const rows = fs.readFileSync("/tmp/pos_session_rows.json", "utf8")
    .split("\n")
    .map(l => l.trim())
    .filter(Boolean)
    .map(l => JSON.parse(l));

  console.log(`Read ${rows.length} rows to restore`);

  const hubDb = knex({
    client: "mysql2",
    connection: parseConn(process.env.LOCAL_URL),
    pool: { min: 0, max: 2 },
  });

  try {
    for (const r of rows) {
      // Normalize types for insertion
      const insertRow = {
        record_id: r.record_id,
        user: r.user,
        store: r.store,
        opening_balance: r.opening_balance,
        closing_balance: r.closing_balance,
        total_amount_sale: r.total_amount_sale,
        opened_at: r.opened_at,
        closed_at: r.closed_at,
        status: r.status,
      };
      // Only include created_at / updated_at if present in payload
      if (r.created_at) insertRow.created_at = r.created_at;
      if (r.updated_at) insertRow.updated_at = r.updated_at;

      await hubDb("pos_session").insert(insertRow).onConflict("record_id").merge();
      console.log(`  restored ${r.record_id} (${r.store.slice(0,8)})`);
    }
    console.log("\nDone. Verifying...");
    const n = await hubDb("pos_session").count("* as c").first();
    console.log(`pos_session on hub now has ${n.c} row(s)`);
  } finally {
    await hubDb.destroy();
  }
}

main().catch(e => { console.error(e); process.exit(1); });
