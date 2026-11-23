# Replica Directory Manual: How to Use Every File, Script, and Workflow

This guide explains how to configure, run, monitor, repair, and recover the hub-and-spoke database replication system using every file in the `replica` directory.

---

## Directory Contents at a Glance

```text
replica/
├── server.js               # Main hub-and-spoke sync daemon, HTTP /health & /resume API, log-fidelity check
├── triggers.js             # Trigger installation, trigger verification, PK & column discovery helpers
├── pk-codec.js             # Composite primary key decoder (JSON array + legacy dash-delimited fallback)
├── node-copy.js            # All-in-one CLI tool (--ping, --verify, --verify-fk, --verify-fk-orphans,
│                           #   --fix-logs, --reseed, --new-branch, --reconcile)
├── fix-drift.js            # Planned cross-node drift diff/insert/collision/content-diff tool
├── sanitize.js             # Source-of-truth full-table diff & reconcile tool
├── restore-from-log.js     # Row state reconstruction & recovery from sym_change_log
├── reconcile.js            # Local-only operational table cleanup (pos_session, cash_count, etc.)
├── watch-dog.js            # Standalone orphan-write detector comparing UPDATE_TIME vs sym_change_log
├── check-env.sh            # Validates .env keys, URLs, and passwords before starting/restarting PM2
├── check-triggers.sh       # Audits sym_trig_* counts, missing tables, and orphan triggers across nodes
├── test-relay.sh           # End-to-end propagation & cleanup test across branch 02 -> hub -> branch 03
├── health-check.sh         # Cron-ready /health polling script with logging, mail, and desktop alerts
├── health.html             # Browser UI for querying /health with an x-api-key saved in localStorage
├── verify-daily.sh         # Daily node-copy.js --verify runner with drift alerts and log rotation
├── backup-daily.sh         # Daily hub mysqldump of ALLOWED_TABLES into backups/ with 30-day retention
├── prune-log.sh            # Deletes old sym_change_log rows on the hub and logs pruned counts
├── morning-report.sh       # Daily 07:00 summary report combining PM2, /health, logs, and row counts
├── install-automation.sh   # Idempotently installs cron jobs for healthcheck, verify, backup, prune, report
├── package.json            # Dependencies and npm scripts (start, ping, triggers)
├── package-lock.json       # Locked dependency tree
├── README.md               # Repository readme
├── node_modules/           # Installed Node packages
└── backups/                # SQL backup dumps created by backup-daily.sh and node-copy.js
```

---

## 1. Environment & Configuration Setup (`.env` & `check-env.sh`)

Almost all scripts in this repository read configuration from `.env` in the current working directory.

### Key `.env` Variables

| Variable | Used By | Purpose & Notes |
| :--- | :--- | :--- |
| `LOCAL_ID` | `server.js`, `triggers.js`, `node-copy.js`, `sanitize.js`, `restore-from-log.js`, `reconcile.js`, `watch-dog.js` | Hub node identifier. Defaults to `NODE_MAIN_HUB`. |
| `LOCAL_CLIENT` | `server.js`, `triggers.js`, `node-copy.js`, `sanitize.js`, `restore-from-log.js`, `reconcile.js` | Hub DB driver (`mysql2` or `pg`). Defaults to `mysql2`. |
| `LOCAL_URL` | All DB scripts | Hub connection URI (e.g., `mysql://user:pass@host:3306/db`). URL-encode special characters like `@` as `%40` in passwords. |
| `LOCAL_SSL`, `LOCAL_SSL_STRICT` | `server.js`, `triggers.js` | Enable SSL or strict certificate verification for the hub connection (`true` / `false`). |
| `BRANCHES_JSON` | All DB scripts | JSON array of branch objects: `[{"id":"NODE_BRANCH_02","client":"mysql2","url":"mysql://..."}]`. |
| `SYNC_TOKEN_<NODE>` | `triggers.js`, `server.js`, `fix-drift.js` | Per-node bypass token (minimum 32 chars; generate with `openssl rand -hex 32`). Key strips leading `NODE_` (for example, `NODE_MAIN_HUB` -> `SYNC_TOKEN_MAIN_HUB`, `NODE_BRANCH_02` -> `SYNC_TOKEN_BRANCH_02`). |
| `ALLOWED_TABLES` | `server.js`, `triggers.js`, `node-copy.js`, `watch-dog.js`, shell scripts | Comma-separated list of tables participating in replication. |
| `ALLOWED_COLUMNS_JSON` | `server.js`, `triggers.js` | Optional JSON object mapping `table_name` to an array of allowed column names (`{"users":["id","name"]}`). |
| `API_SECRET_KEY` | `server.js`, `health-check.sh`, `morning-report.sh` | Shared secret sent in the `x-api-key` header for `/health` and `/resume/:branchId`. |
| `PAYLOAD_ENCRYPTION_KEY` | `server.js` | Optional 64-character hex (32-byte) key for AES-256-GCM decryption of `row_data`. |
| `PORT` | `server.js` | HTTP port for the monitoring API (default `3000`). Note: `health-check.sh` and `morning-report.sh` default to `http://localhost:8182/health` unless overridden via `HEALTH_URL` or edited. |
| `POLL_INTERVAL_MS` | `server.js` | Idle polling interval in ms (default `2000`, minimum `250`). |
| `BATCH_SIZE` | `server.js` | Max `sym_change_log` rows read per batch (default `500`). |
| `UPSERT_CHUNK_SIZE` | `server.js` | Row chunk size for inserts/upserts/relays (default `200`). |
| `RELAY_ENABLED` | `server.js` | When `true` on the hub, re-emits changes applied to the hub into `sym_change_log` with `node_source_id` set to the hub so other branches receive them (default `true`). |
| `VERIFY_AFTER_APPLY` | `server.js` | Verifies that applied rows actually landed on the target before advancing the checkpoint; halts the worker on mismatch (default `true`). |
| `SETUP_TRIGGERS_ON_STARTUP` | `server.js` | If `true`, reinstalls triggers on every node during startup (default `false`; keep `false` in production unless intended). |
| `FAIL_ON_MISSING_TRIGGERS` | `server.js` | If `true`, refuses to start when trigger verification reports missing triggers (default `true`). |
| `DEFER_MAX_ATTEMPTS`, `DEFER_MAX_QUEUE_SIZE` | `server.js` | Retry limit (default `5`) and max queue size (default `10000`) for deferrable foreign-key errors. |
| `LOG_FIDELITY_INTERVAL_MS`, `LOG_FIDELITY_ALERT_LAG_MS`, `LOG_FIDELITY_WEBHOOK_URL` | `server.js`, `watch-dog.js` | Cadence, lag threshold, and optional JSON webhook URL for detecting writes outside `sym_change_log`. |
| `BACKUP_DIR`, `BACKUP_RETENTION_DAYS` | `node-copy.js` | Backup output directory (default `./backups`) and retention age in days (default `7`). |
| `LOCAL_ONLY_TABLES`, `BRANCH_STORE_KEY`, `BRANCH_STORE_TABLE`, `BRANCH_STORE_NAME` | `reconcile.js` | Defaults for local-only operational tables (`pos_session,cash_count`) and store mapping (`store`, `store`, `name`). |
| `PRUNE_DAYS` | `prune-log.sh` | Age threshold in days for deleting old `sym_change_log` entries (default `90`). |
| `MAILTO` | `verify-daily.sh`, `health-check.sh` | Optional email recipient when drift or an unhealthy status occurs and `mail` is available. |

### Validating `.env` with `check-env.sh`

Run `check-env.sh` before starting or restarting `server.js` / PM2:

```bash
./check-env.sh
```

It checks that `BRANCHES_JSON` is valid JSON with non-empty `id` and `url` fields, verifies `LOCAL_URL` and `ALLOWED_TABLES`, ensures `API_SECRET_KEY` is present, and flags unencoded `@` characters in database passwords (which must be `%40`). Exit status `0` means valid; non-zero means fix the listed errors first.

---

## 2. How to Use Every File

### `triggers.js` — Install & Verify Database Triggers

`triggers.js` can be used both as a CLI tool and as a module imported by `server.js` and `node-copy.js`.

* **CLI flags:**
  * `--verify` — Checks trigger counts per table in `ALLOWED_TABLES` without modifying the database.
  * `--node=ID` — Restricts execution to a single node ID (such as `NODE_MAIN_HUB` or `NODE_BRANCH_02`).
  * `--table=NAME` — Restricts execution to a single table (must be in `ALLOWED_TABLES`).
  * `--help` or `-h` — Prints CLI usage.

* **Examples:**

```bash
# Install/recreate triggers on all configured nodes
node triggers.js
npm run triggers

# Verify triggers across all nodes
node triggers.js --verify

# Install or verify triggers for one table on one node
node triggers.js --node=NODE_BRANCH_02 --table=customer
node triggers.js --node=NODE_BRANCH_02 --table=customer --verify
```

### `server.js` — Run the Hub-and-Spoke Sync Engine & Monitoring API

`server.js` provisions `sym_change_log` and `sym_checkpoint` if missing, verifies triggers, drains existing backlog via `flushPendingSyncs`, starts per-branch `BranchSyncManager` loops (`hubToBranch` and `branchToHub`), runs background `logFidelityCheck`, and exposes HTTP endpoints.

* **Starting the engine:**

```bash
npm start
# or
node server.js
# or managed with PM2:
pm2 start server.js --name replica-engine
```

* **HTTP endpoints (both require `x-api-key: <API_SECRET_KEY>`):**
  * `GET /health` — Returns hub ID, overall `healthy`, `anyHalted`, `activeBranches`, `totalDeferredRows`, `relayEnabled`, and per-branch status (`lastProcessedChangeId`, `hubToBranchLag`, `rowCounts`, `deferredQueueDepth`, `halted`, `consecutiveErrors`, `lastError`, `stats`).
  * `POST /resume/:branchId` — Clears the halted state and deferred queue on `hubToBranch` and `branchToHub` for `:branchId` so syncing resumes after manual intervention. Returns `404` if `branchId` is unknown.

### `pk-codec.js` — Composite Primary Key Decoding

`pk-codec.js` exports `decodeCompositeKey(rawValue, expectedColumnCount, tableName)`.

* If `rawValue` starts with `[`, it parses it as a JSON array and verifies that its length equals `expectedColumnCount`, returning stringified values (or `null` for `null`/`undefined`).
* Otherwise, it splits on `-` and verifies the segment count equals `expectedColumnCount`. If a UUID or negative number contains `-` and causes a count mismatch, it throws an explanatory error instructing you to re-run trigger setup so composite keys use JSON-array encoding.

### `node-copy.js` — Multi-Mode Operational CLI

`node-copy.js` is the primary maintenance CLI for MySQL/MariaDB nodes.

#### Modes

1. **`--ping`** — Tests connectivity (`SELECT 1`) to all configured nodes within `CHECK_TIMEOUT_MS` and prints a status table. Exits `0` if all nodes are online, `1` otherwise.
2. **`--verify`** — Compares hub vs. branch primary keys and (when `--full` is enabled, which is the default) checks column values on matching rows.
3. **`--verify-fk`** — Compares foreign-key relationships (`child.col -> parent.col`) across all nodes against the cluster union.
4. **`--verify-fk-orphans`** — Scans foreign keys on all nodes for non-empty/non-null/non-zero child values whose parent row is missing.
5. **`--fix-logs`** — Finds `sym_change_log` rows on a node whose `node_source_id` matches another known node ID instead of that node's own ID. Dry-run by default; pass `--apply` to relabel.
6. **`--reseed`** — Performs a 7-step destructive reset of target table data from `--from` to `--to` (`<nodeId>` or `all`):
   1. Backs up target tables to `./backups/` (unless `--no-backup`).
   2. Drops `sym_trig_%` triggers on target (unless `--skip-triggers`).
   3. Truncates target tables with `FOREIGN_KEY_CHECKS = 0`.
   4. Truncates `sym_change_log` on source and target (unless `--keep-log`).
   5. Streams table data from source to target via `mysqldump` piped into `mysql`.
   6. Rebuilds triggers on target via `setupNodeTriggers` (unless `--skip-triggers`).
   7. Truncates `sym_checkpoint` on target (unless `--keep-checkpoint`).
7. **`--new-branch`** — Provisions a new branch (`--to=<branchId>`, defaulting `--from` to the hub) while keeping existing branches untouched:
   * Backs up target, records source max `change_id`, dumps source excluding `sym_change_log` and `sym_checkpoint`, drops target triggers, truncates and restores target data, deletes inherited open `pos_session` (`status = 1`) and today's `cash_count`, rebuilds triggers, inserts the new `store` row + zero-quantity `stocks_on_hand` rows for active `product` records + optional `store_user` assignments, and sets `sym_checkpoint` (with a `10000` ID buffer) so historical hub events are not replayed while new branch records sync forward.
8. **`--reconcile`** — Scores nodes (`totalRows - orphanCount * 100`), selects the highest-scoring source (or uses `--trust=<id>` / `--trust=hub` if top scores are within `--min-score-delta`, default `0.15`), optionally pulls unique rows for `--enrich-tables=...` into the source, and reseeds other nodes when `--apply` is passed.

#### Common `node-copy.js` Flags

* `--from=<id>`, `--to=<id|all>`
* `--tables=t1,t2` (filtered against `ALLOWED_TABLES`)
* `--apply` / `--dry-run`
* `--no-backup`, `--skip-triggers`, `--keep-log`, `--keep-checkpoint`, `--skip-preflight`
* `--full` / `--no-full`, `--max-report=N`
* `--trust=<id|hub>`, `--min-score-delta=0.15`, `--enrich-tables=t1,t2`
* `--new-branch` options: `--store-name=...`, `--store-address=...`, `--store-contact=...`, `--store-contact-person=...`, `--store-other-info=...`, `--store-uuid=...`, `--store-users=u1,u2`, `--primary-user=u1`

#### Examples

```bash
node node-copy.js --ping
node node-copy.js --verify
node node-copy.js --verify-fk
node node-copy.js --verify-fk-orphans
node node-copy.js --fix-logs --apply
node node-copy.js --reseed --from=NODE_MAIN_HUB --to=NODE_BRANCH_02
node node-copy.js --reseed --from=NODE_MAIN_HUB --to=NODE_BRANCH_02 --apply
node node-copy.js --new-branch --from=NODE_MAIN_HUB --to=NODE_BRANCH_03 \
  --store-name="BRANCH 03" --store-address="Poblacion, Manga" \
  --store-contact="0917-123-4567" --store-contact-person="Maria" --apply
```

### `fix-drift.js` — Planned Drift Repair with Collision & Corruption Guards

`fix-drift.js` uses a built-in `TABLE_PLAN` (covering `customer`, `repa_deposit`, `repa_member`, `repa_member_winner`, `sales_order`, `sales_order_details`, `sales_payment`, `deposit`, `discount`, `stock_requests`, `stock_request_items`) to plan and apply missing row inserts, handle unique-column collisions, and inspect MD5 row content diffs.

* **Flags:**
  * `--apply` — Executes planned row inserts (exits `1` in dry-run mode when run without `--apply`).
  * `--apply-content` — Applies content diffs when `contentAuthority` is configured for a table and the corruption guard allows it.
  * `--content-authority=table:nodeId` — Sets the winning node for content diffs on `table` (for example, `--content-authority=customer:NODE_MAIN_HUB`).
  * `--collision=prefix|null|skip` — How to resolve unique-key collisions on the source node before inserting (default `prefix`, which appends a short node suffix).
  * `--only-safe` — Filters to tables marked `safeToAutoApply: true` in `TABLE_PLAN`.
  * `--force-bulk` — Allows applying content diffs even when `BULK_TIMESTAMP` is detected.
  * `--tables=t1,t2`, `--batch-size=500`, `--verbose`, `--help`.

* **Pre-apply gates (`--apply`):**
  * Requires `replica-engine` to be `online` in `pm2` (and if restarted, up for at least 10 minutes).
  * Requires a `<1h` old `.sql` backup file in `./backups/` matching each planned table name.
  * Sets `@sym_sync_token` on the target connection during writes and runs `node node-copy.js --verify` after applying.

* **Examples:**

```bash
# Dry-run inspection
node fix-drift.js --tables=customer,sales_order --verbose

# Apply safe tables only
node fix-drift.js --only-safe --apply

# Apply with collision renaming and hub content authority
node fix-drift.js --tables=customer --collision=prefix \
  --content-authority=customer:NODE_MAIN_HUB --apply --apply-content
```

### `sanitize.js` — Source-of-Truth Table Reconciliation

`sanitize.js` compares full rows of specified tables between `--source_id` (or alias `hub`) and one or more `--targets`, computing rows to **insert**, **update**, and **delete** so targets match the source.

* **Flags:**
  * `--tables=T1,T2` (required)
  * `--source_id=ID` (required; supports `hub` or exact node ID)
  * `--targets=ID1,ID2` (required)
  * `--apply` / `--dry-run` (default is dry-run)
  * `--batch-size=N` (default `200`)

* **Exit codes:** `0` = in sync or applied cleanly; `1` = drift detected during dry-run; `2` = invalid arguments, missing node, or database error.

* **Examples:**

```bash
# Dry run
node sanitize.js --tables=pos_session --source_id=hub --targets=NODE_BRANCH_02,NODE_BRANCH_03

# Apply inside a transaction
node sanitize.js --tables=pos_session --source_id=hub --targets=NODE_BRANCH_02,NODE_BRANCH_03 --apply
```

### `restore-from-log.js` — Rebuild Rows from `sym_change_log`

`restore-from-log.js` reads events from `sym_change_log` on `--from`, reconstructs the latest state per primary key (latest `change_id` wins; `INSERT`/`UPDATE` marks PK `present` with `row_data`, `DELETE` marks PK `absent`), and upserts or deletes rows on `--to` targets.

* **Flags:**
  * `--tables=T1,T2` (required)
  * `--from=ID` (required)
  * `--to=ID1,ID2` (required; may include the source node)
  * `--operation=INSERT|DELETE|BOTH` (default `INSERT`)
  * `--since=YYYY-MM-DD`, `--until=YYYY-MM-DD`
  * `--record-ids=ID1,ID2`
  * `--batch-size=N` (default `200`)
  * `--dry-run` (default) / `--apply`

* **Examples:**

```bash
# Preview restoring deleted pos_session rows from hub since 2026-11-11
node restore-from-log.js --tables=pos_session --from=hub --to=hub \
  --operation=DELETE --since=2026-11-11

# Apply restore to hub and branches
node restore-from-log.js --tables=pos_session,cash_count --from=hub \
  --to=hub,NODE_BRANCH_02,NODE_BRANCH_03 --operation=DELETE --since=2026-11-11 --apply
```

### `reconcile.js` — Clean Up Local-Only Operational Tables

`reconcile.js` inspects and reconciles tables that are meant to stay local per store/branch (such as `pos_session` and `cash_count`) rather than replicating across all branches.

* **Flags:**
  * `--tables=T1,T2` or `--all` (defaults to `LOCAL_ONLY_TABLES`, i.e., `pos_session,cash_count`)
  * `--inspect` — Prints total and per-store row counts per node without building or running a plan
  * `--apply` / `--dry-run` (default is dry-run)
  * `--skip-drop-triggers` — Keeps `sym_trig_<table>_%` triggers on those tables
  * `--skip-log-cleanup` — Keeps `sym_change_log` rows for those tables
  * `--yes` or `-y` — Skips the interactive `"yes"` confirmation prompt

* **Examples:**

```bash
node reconcile.js --inspect
node reconcile.js --tables=pos_session,cash_count
node reconcile.js --tables=pos_session,cash_count --apply --yes
```

### `watch-dog.js` — Detect Unlogged ("Orphan") Writes

`watch-dog.js` loads `.env` directly, connects to the hub and all `BRANCHES_JSON` nodes, and compares each `ALLOWED_TABLES` table's `information_schema.TABLES.UPDATE_TIME` against `MAX(sym_change_log.created_at)` for that table.

* If `UPDATE_TIME - MAX(created_at) > LOG_FIDELITY_ALERT_LAG_MS` (default `300000` ms = 5 minutes), it flags the table as having an orphan write, prints a report to `stderr`, optionally POSTs JSON `{ text, findings }` to `LOG_FIDELITY_WEBHOOK_URL`, and exits with code `1`.
* Exits `0` when all nodes are clean, or `2` on unhandled error.

```bash
node watch-dog.js
```

---

## 3. Shell Scripts, Monitoring & Automation

### Shell Script Reference

| Script | How to Run | What It Does |
| :--- | :--- | :--- |
| `check-env.sh` | `./check-env.sh` | Validates `.env` configuration and password `@` encoding before PM2 restart. |
| `check-triggers.sh` | `./check-triggers.sh` | Audits trigger counts (`ins`/`upd`/`del`), missing triggers, and orphan triggers across HUB, BR02, and BR03. |
| `test-relay.sh` | `./test-relay.sh` | Inserts a test `pos_session` row on branch 02, waits 8s, checks BR02, HUB, and BR03, deletes the row, waits 6s, and verifies deletion propagated. |
| `health-check.sh` | `./health-check.sh` | Queries `/health` with `x-api-key`, logs unhealthy alerts to `healthcheck.log` (and `OK` at `:00`), and optionally triggers `mail` or `notify-send`. |
| `verify-daily.sh` | `./verify-daily.sh` | Runs `node node-copy.js --verify`, logs drift to `verify-daily.log`, optionally emails `$MAILTO`, and trims logs over 10 MB to the last 1000 lines. |
| `backup-daily.sh` | `./backup-daily.sh` | Runs `mysqldump` (`--single-transaction --no-create-info --complete-insert`) for `ALLOWED_TABLES` on the hub into `backups/hub-<timestamp>.sql`, logs to `backup-daily.log`, and keeps the newest 30 files. |
| `prune-log.sh` | `./prune-log.sh` | Deletes `sym_change_log` rows older than `PRUNE_DAYS` (default `90`) on the hub and logs the row count to `prune-log.log`. |
| `morning-report.sh` | `./morning-report.sh` | Generates and prints `morning-report.txt` containing `pm2 status replica-engine`, `/health` summary, last 3 lines of `healthcheck.log` / `verify-daily.log` / `backup-daily.log`, and `pos_session` row counts. |
| `install-automation.sh` | `./install-automation.sh` | Removes any old cron lines referencing the current directory and installs cron entries for `healthcheck.sh` (every 15m), `verify-daily.sh` (03:00 daily), `backup-daily.sh` (03:05 daily), `prune-log.sh` (04:00 on the 1st of each month), and `morning-report.sh` (07:00 daily). |

### Using `health.html`

Open `health.html` in a browser (ideally served from the same origin as the hub API so browser CORS policies do not block `fetch`):

1. Enter the `/health` endpoint URL (default `http://localhost:8182/health`—update to match your `server.js` `PORT` or reverse proxy URL).
2. Paste `API_SECRET_KEY` into the API Key field (saved automatically in `localStorage` under `replica_api_key`).
3. Click **Fetch** (or press `Enter`) to view overall health, halt status, deferred row count, per-branch status/errors, and expandable raw JSON.

---

## 4. End-to-End Playbooks & Use Cases

### Playbook 1: Initial Bring-Up & Daily Operations

1. Validate `.env`: `./check-env.sh`
2. Verify/install triggers: `node triggers.js --verify` (or `node triggers.js`)
3. Start the sync engine: `pm2 start server.js --name replica-engine`
4. Verify connectivity and relay: `node node-copy.js --ping` and `./test-relay.sh`
5. Install scheduled monitoring/backups: `./install-automation.sh`
6. Check daily status via `./morning-report.sh` or `health.html`.

### Playbook 2: Investigating & Recovering from a Halted Branch Worker

1. Poll `/health` (or open `health.html`) and inspect `anyHalted`, `halted.reason`, `deferredQueueDepth`, and `lastError`.
2. Check foreign keys and data integrity (`node node-copy.js --verify-fk-orphans` or `node node-copy.js --verify`).
3. Repair missing/conflicting rows using `fix-drift.js`, `sanitize.js`, or `restore-from-log.js` as appropriate.
4. Resume the branch worker via `POST /resume/:branchId` and watch `/health` to confirm `halted` clears and `hubToBranchLag` drains.

### Playbook 3: Recovering After Accidental Deletes or Bad Drift

* **Recent accidental deletes captured in `sym_change_log`:** Run `restore-from-log.js` with `--operation=DELETE` (and `--since` / `--until` / `--record-ids`) in dry-run mode first, then with `--apply`.
* **Target table out of parity with a trusted source:** Preview with `sanitize.js` (or `fix-drift.js` for planned tables), then execute with `--apply`.
* **Severe corruption or new branch rollout:** Use `node-copy.js --reseed` (with sync stopped and backups enabled) or `node-copy.js --new-branch` for adding a new branch without disturbing existing ones.