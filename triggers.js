require("dotenv").config();
const knex = require("knex");

function defaultLogger() {
  return {
    info: (...a) => console.log(...a),
    warn: (...a) => console.warn(...a),
    error: (...a) => console.error(...a),
    debug: (...a) => {
      if (process.env.NODE_ENV !== "production") console.debug(...a);
    },
  };
}
let LOGGER = defaultLogger();
function setLogger(l) {
  if (l) LOGGER = l;
}

const EXPECTED_TRIGGERS_PER_TABLE = 3;

const rawAllowedTables = process.env.ALLOWED_TABLES || "";
const ALLOWED_TABLES = new Set(
  rawAllowedTables
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean),
);

const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
function assertSafeIdentifier(name, kind) {
  if (typeof name !== "string" || !IDENT_RE.test(name)) {
    throw new Error(
      `Unsafe ${kind} identifier ${JSON.stringify(name)}: must match ${IDENT_RE}`,
    );
  }
  return name;
}
function quoteMysqlIdent(name) {
  assertSafeIdentifier(name, "MySQL");
  return `\`${name}\``;
}
function quotePgIdent(name) {
  assertSafeIdentifier(name, "PostgreSQL");
  return `"${name}"`;
}
function escapeSqlString(s) {
  return String(s).replace(/'/g, "''");
}

function getSyncTokenForNode(nodeId) {
  const key = `SYNC_TOKEN_${String(nodeId).replace(/^NODE_/, "")}`;
  const v = process.env[key];
  if (!v || v.length < 32) {
    throw new Error(
      `Missing/short ${key} in environment. Generate with: openssl rand -hex 32.`,
    );
  }
  return v;
}

const pkCache = new Map();
async function getTablePkColumns(node, tableName) {
  assertSafeIdentifier(tableName, "table");
  const cacheKey = `${node.id}:${tableName}`;
  if (pkCache.has(cacheKey)) return pkCache.get(cacheKey);
  const promise = (async () => {
    const db = node.db;
    const client = node.client;
    let pkColumns = [];
    if (client === "pg" || client === "postgres") {
      const res = await db.raw(
        `SELECT a.attname FROM pg_index i
         JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
         WHERE i.indrelid = ?::regclass AND i.indisprimary ORDER BY k.ord;`,
        [tableName],
      );
      pkColumns = res.rows.map((r) => r.attname);
    } else {
      const res = await db.raw(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [
        tableName,
      ]);
      const rows = res[0] || res;
      pkColumns = rows
        .slice()
        .sort((a, b) => Number(a.Seq_in_index) - Number(b.Seq_in_index))
        .map((r) => r.Column_name);
    }
    return pkColumns;
  })();
  promise.catch(() => pkCache.delete(cacheKey));
  pkCache.set(cacheKey, promise);
  return promise;
}
function clearPkCache(nodeId) {
  if (!nodeId) {
    pkCache.clear();
    return;
  }
  for (const k of pkCache.keys())
    if (k.startsWith(`${nodeId}:`)) pkCache.delete(k);
}

async function getTableColumns(node, tableName) {
  assertSafeIdentifier(tableName, "table");
  if (node.client === "pg" || node.client === "postgres") {
    const res = await node.db.raw(
      `SELECT column_name FROM information_schema.columns
       WHERE table_name = ? AND table_schema = current_schema()
       ORDER BY ordinal_position;`,
      [tableName],
    );
    return res.rows.map((r) => r.column_name);
  }
  const res = await node.db.raw(
    `SELECT COLUMN_NAME FROM information_schema.columns
     WHERE table_name = ? AND table_schema = DATABASE()
     ORDER BY ORDINAL_POSITION;`,
    [tableName],
  );
  return (res[0] || res).map((r) => r.COLUMN_NAME);
}

async function verifyNodeTriggers(
  node,
  expectedPerTable = EXPECTED_TRIGGERS_PER_TABLE,
) {
  const tables = [...ALLOWED_TABLES];
  try {
    if (node.client === "pg" || node.client === "postgres") {
      const exp = 1;
      const res = await node.db.raw(
        `SELECT event_object_table AS t, COUNT(*) AS n
         FROM information_schema.triggers
         WHERE trigger_schema = current_schema() AND event_object_table = ANY(?)
         GROUP BY event_object_table`,
        [tables],
      );
      const rows = res.rows || res;
      const have = new Map(rows.map((r) => [r.t, Number(r.n)]));
      const missing = tables
        .filter((t) => (have.get(t) || 0) < exp)
        .map((t) => ({ table: t, have: have.get(t) || 0, need: exp }));
      return { ok: missing.length === 0, missing, extra: [], expected: exp };
    }
    const res = await node.db.raw(
      `SELECT EVENT_OBJECT_TABLE AS t, COUNT(*) AS n
       FROM information_schema.TRIGGERS
       WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE IN (?)
       GROUP BY EVENT_OBJECT_TABLE`,
      [tables],
    );
    const rows = res[0] || res;
    const have = new Map(rows.map((r) => [r.t, Number(r.n)]));
    const missing = tables
      .filter((t) => (have.get(t) || 0) < expectedPerTable)
      .map((t) => ({
        table: t,
        have: have.get(t) || 0,
        need: expectedPerTable,
      }));
    const extra = [];
    for (const [t, n] of have)
      if (n > expectedPerTable)
        extra.push({ table: t, have: n, need: expectedPerTable });
    return {
      ok: missing.length === 0,
      missing,
      extra,
      expected: expectedPerTable,
    };
  } catch (err) {
    return { ok: false, error: err.message, missing: [], extra: [] };
  }
}

function getAllowedColumnsForTable(tableName) {
  try {
    const raw = process.env.ALLOWED_COLUMNS_JSON;
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const list = parsed[tableName];
    return Array.isArray(list) && list.length ? list : null;
  } catch {
    return null;
  }
}

async function setupNodeTriggers(node) {
  const summary = { ok: [], failed: [], skipped: [], drift: [] };
  const isPg = node.client === "pg" || node.client === "postgres";
  let token;
  try {
    token = getSyncTokenForNode(node.id);
  } catch (err) {
    LOGGER.error(`[Triggers] ${node.id}: ${err.message}`);
    summary.failed.push({ table: "*", error: err.message });
    return summary;
  }
  LOGGER.info(
    `[Triggers] ${node.id} (${node.client}): installing for ${ALLOWED_TABLES.size} tables, token=${token.slice(0, 8)}…`,
  );
  for (const tableName of ALLOWED_TABLES) {
    try {
      assertSafeIdentifier(tableName, "table");
      const pkColumns = await getTablePkColumns(node, tableName);
      if (!pkColumns || pkColumns.length === 0) {
        summary.skipped.push({ table: tableName, reason: "no primary key" });
        continue;
      }
      const columns = await getTableColumns(node, tableName);
      if (columns.length === 0) {
        summary.skipped.push({ table: tableName, reason: "table not found" });
        continue;
      }
      if (isPg)
        await setupPostgresTriggers(
          node.db,
          node.id,
          tableName,
          pkColumns,
          columns,
          token,
        );
      else
        await setupMysqlTriggers(
          node.db,
          node.id,
          tableName,
          pkColumns,
          columns,
          token,
        );
      summary.ok.push({ table: tableName, pkColumns });
    } catch (err) {
      LOGGER.error(`[Triggers] ${node.id} / ${tableName}: ${err.message}`);
      summary.failed.push({ table: tableName, error: err.message });
    }
  }
  const v = await verifyNodeTriggers(
    node,
    isPg ? 1 : EXPECTED_TRIGGERS_PER_TABLE,
  );
  if (!v.ok) {
    summary.drift.push(v);
    LOGGER.error(
      `[Triggers] ${node.id}: post-install drift: missing=${v.missing.length} extra=${v.extra.length}`,
    );
  } else
    LOGGER.info(
      `[Triggers] ${node.id}: post-install verify OK (${ALLOWED_TABLES.size} tables × ${v.expected})`,
    );
  return summary;
}

async function setupMysqlTriggers(
  db,
  nodeId,
  tableName,
  pkColumns,
  allColumns,
  token,
) {
  const allowList = getAllowedColumnsForTable(tableName);
  const columns = allowList
    ? allColumns.filter((c) => allowList.includes(c))
    : allColumns;
  if (!columns.length)
    throw new Error(`No columns to serialize for '${tableName}'`);
  const buildPkVal = (p) =>
    pkColumns.length === 1
      ? `CAST(${p}.${quoteMysqlIdent(pkColumns[0])} AS CHAR)`
      : `CAST(JSON_ARRAY(${pkColumns.map((c) => `CAST(${p}.${quoteMysqlIdent(c)} AS CHAR)`).join(", ")}) AS CHAR)`;
  const buildJsonObject = (p) => {
    const pairs = columns.map((col) => {
      const id = quoteMysqlIdent(col);
      const v =
        col === "updated_at" || col === "created_at"
          ? `DATE_FORMAT(${p}.${id}, '%Y-%m-%d %H:%i:%s')`
          : `${p}.${id}`;
      return `'${escapeSqlString(col)}', ${v}`;
    });
    return `JSON_OBJECT(${pairs.join(", ")})`;
  };
  const tableIdent = quoteMysqlIdent(tableName);
  const srcLit = `'${escapeSqlString(nodeId)}'`;
  const tblLit = `'${escapeSqlString(tableName)}'`;
  const tokLit = `'${escapeSqlString(token)}'`;
  const tI = `sym_trig_${tableName}_ins`,
    tU = `sym_trig_${tableName}_upd`,
    tD = `sym_trig_${tableName}_del`;
  await db.raw(`DROP TRIGGER IF EXISTS ??`, [tI]);
  await db.raw(`DROP TRIGGER IF EXISTS ??`, [tU]);
  await db.raw(`DROP TRIGGER IF EXISTS ??`, [tD]);
  const mk = (name, op, nv) => `
    CREATE TRIGGER ${quoteMysqlIdent(name)} AFTER ${op} ON ${tableIdent}
    FOR EACH ROW
    BEGIN
      IF @sym_sync_token IS NULL OR @sym_sync_token <> ${tokLit} THEN
        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES (${tblLit}, '${op}', ${buildPkVal(nv)}, ${buildJsonObject(nv)}, ${srcLit});
      END IF;
    END;`;
  try {
    await db.raw(mk(tI, "INSERT", "NEW"));
    await db.raw(mk(tU, "UPDATE", "NEW"));
    await db.raw(mk(tD, "DELETE", "OLD"));
  } catch (err) {
    if (
      /ER_BINLOG_CREATE_ROUTINE_NEED_SUPER|SUPER privilege/i.test(
        err.message,
      ) ||
      err.code === "ER_BINLOG_CREATE_ROUTINE_NEED_SUPER"
    ) {
      throw new Error(
        `${err.message}\nHint: SET GLOBAL log_bin_trust_function_creators = 1; or grant SUPER.`,
      );
    }
    throw err;
  }
}

async function setupPostgresTriggers(
  db,
  nodeId,
  tableName,
  pkColumns,
  allColumns,
  token,
) {
  const allowList = getAllowedColumnsForTable(tableName);
  const columns = allowList
    ? allColumns.filter((c) => allowList.includes(c))
    : allColumns;
  if (!columns.length)
    throw new Error(`No columns to serialize for '${tableName}'`);
  const funcName = `sym_fn_${tableName}_change`,
    triggerName = `sym_trig_${tableName}`;
  assertSafeIdentifier(funcName, "function");
  assertSafeIdentifier(triggerName, "trigger");
  const funcIdent = quotePgIdent(funcName),
    triggerIdent = quotePgIdent(triggerName),
    tableIdent = quotePgIdent(tableName);
  await db.raw(`DROP TRIGGER IF EXISTS ${triggerIdent} ON ${tableIdent};`);
  await db.raw(`DROP FUNCTION IF EXISTS ${funcIdent}();`);
  const pkBuild =
    pkColumns.length === 1
      ? `CAST(target_record.${quotePgIdent(pkColumns[0])} AS TEXT)`
      : `json_build_array(${pkColumns.map((c) => `CAST(target_record.${quotePgIdent(c)} AS TEXT)`).join(", ")})::text`;
  const rowJson =
    columns.length > 0 && allowList
      ? `json_build_object(${columns.map((c) => `'${escapeSqlString(c)}', target_record.${quotePgIdent(c)}`).join(", ")})::text`
      : `row_to_json(target_record)::text`;
  const srcLit = `'${escapeSqlString(nodeId)}'`,
    tblLit = `'${escapeSqlString(tableName)}'`,
    tokLit = `'${escapeSqlString(token)}'`;
  const fnBody = `
    CREATE OR REPLACE FUNCTION ${funcIdent}() RETURNS TRIGGER AS $$
    DECLARE current_token TEXT; target_record RECORD; pk_val TEXT; row_json TEXT;
    BEGIN
      BEGIN current_token := current_setting('sym.sync_token', true);
      EXCEPTION WHEN OTHERS THEN current_token := NULL; END;
      IF current_token IS NULL OR current_token != ${tokLit} THEN
        IF (TG_OP = 'DELETE') THEN target_record := OLD; ELSE target_record := NEW; END IF;
        pk_val := ${pkBuild};
        row_json := ${rowJson};
        INSERT INTO sym_change_log (table_name, operation, primary_key_val, row_data, node_source_id)
        VALUES (${tblLit}, TG_OP, pk_val, row_json, ${srcLit});
      END IF;
      RETURN NULL;
    END; $$ LANGUAGE plpgsql;`;
  await db.raw(fnBody);
  await db.raw(
    `CREATE TRIGGER ${triggerIdent} AFTER INSERT OR UPDATE OR DELETE ON ${tableIdent} FOR EACH ROW EXECUTE FUNCTION ${funcIdent}();`,
  );
}

module.exports = {
  getTablePkColumns,
  getTableColumns,
  setupNodeTriggers,
  verifyNodeTriggers,
  clearPkCache,
  setLogger,
  ALLOWED_TABLES,
  EXPECTED_TRIGGERS_PER_TABLE,
  getSyncTokenForNode,
};

if (require.main === module) {
  (async () => {
    const args = process.argv.slice(2);
    const flag = (n) => {
      const h = args.find((a) => a.startsWith(`--${n}=`));
      return h ? h.slice(n.length + 3) : null;
    };
    if (args.includes("--help") || args.includes("-h")) {
      console.log(
        `Usage: node triggers.js [--node=ID] [--table=NAME] [--verify]`,
      );
      process.exit(0);
    }
    const onlyNode = flag("node"),
      onlyTable = flag("table"),
      verifyOnly = args.includes("--verify");
    if (onlyTable) {
      for (const t of Array.from(ALLOWED_TABLES))
        if (t !== onlyTable) ALLOWED_TABLES.delete(t);
      if (!ALLOWED_TABLES.size) {
        console.error(`--table=${onlyTable} not in ALLOWED_TABLES`);
        process.exit(1);
      }
    }
    function parseConn(u, d) {
      const x = new URL(u);
      return {
        host: x.hostname || "127.0.0.1",
        port: x.port ? parseInt(x.port, 10) : d,
        user: x.username ? decodeURIComponent(x.username) : undefined,
        password: x.password ? decodeURIComponent(x.password) : undefined,
        database: x.pathname.slice(1),
      };
    }
    function norm(c) {
      if (!c) return "mysql2";
      const x = c.toLowerCase().trim();
      if (x === "mysql") return "mysql2";
      if (x === "postgresql") return "pg";
      return x;
    }
    const nodes = [];
    if (process.env.LOCAL_URL)
      nodes.push({
        id: process.env.LOCAL_ID || "NODE_MAIN_HUB",
        client: norm(process.env.LOCAL_CLIENT),
        url: process.env.LOCAL_URL,
        ssl: process.env.LOCAL_SSL === "true",
        sslStrict: process.env.LOCAL_SSL_STRICT !== "false",
      });
    let branches = [];
    try {
      branches = JSON.parse(process.env.BRANCHES_JSON || "[]");
    } catch (e) {
      console.error("Bad BRANCHES_JSON:", e.message);
      process.exit(1);
    }
    for (const b of branches)
      nodes.push({
        id: b.id,
        client: norm(b.client),
        url: b.url,
        ssl: true,
        sslStrict: b.sslInsecure !== true,
      });
    if (!nodes.length) {
      console.warn("No connection strings.");
      process.exit(0);
    }
    let failed = false;
    for (const n of nodes) {
      if (onlyNode && n.id !== onlyNode) continue;
      const isPg = n.client === "pg" || n.client === "postgres";
      const db = knex({
        client: n.client,
        connection: isPg
          ? {
              connectionString: n.url,
              ssl: n.ssl ? { rejectUnauthorized: n.sslStrict } : false,
            }
          : parseConn(n.url, 3306),
        pool: { min: 0, max: 4 },
      });
      try {
        if (verifyOnly) {
          console.log(`[verify] ${n.id} (${n.client})`);
          const v = await verifyNodeTriggers(
            { id: n.id, client: n.client, db },
            isPg ? 1 : EXPECTED_TRIGGERS_PER_TABLE,
          );
          if (v.ok)
            console.log(
              `  OK: all ${ALLOWED_TABLES.size} tables × ${v.expected}`,
            );
          else if (v.error) {
            console.error(`  ERROR: ${v.error}`);
            failed = true;
          } else {
            console.error(
              `  DRIFT: ${v.missing.length} missing, ${v.extra.length} extra`,
            );
            for (const m of v.missing.slice(0, 20))
              console.error(`    MISSING ${m.table} (${m.have}/${m.need})`);
            if (v.missing.length > 20)
              console.error(`    ... +${v.missing.length - 20} more`);
            failed = true;
          }
        } else {
          const s = await setupNodeTriggers({ id: n.id, client: n.client, db });
          console.log(
            `[install] ${n.id}: ok=${s.ok.length} skip=${s.skipped.length} fail=${s.failed.length}`,
          );
          if (s.failed.length) {
            failed = true;
            for (const f of s.failed)
              console.error(`  FAILED ${f.table}: ${f.error}`);
          }
          if (s.drift.length) {
            failed = true;
            for (const d of s.drift) console.error(`  DRIFT after install`);
          }
        }
      } catch (err) {
        failed = true;
        console.error(`[${n.id}] ${err.message}`);
      } finally {
        await db.destroy();
      }
    }
    process.exit(failed ? 1 : 0);
  })().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
