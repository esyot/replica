**Replica** is a lightweight, high-performance Node.js multi-node database synchronization and replication engine built on top of **Knex.js**. 

Designed using a **Hub-and-Spoke topology**, Replica seamlessly syncs data bi-directionally between a main centralized database (Hub) and multiple branch databases (Spokes)—even across cross-database engines (e.g., **PostgreSQL ↔ MySQL**). It operates via database triggers, recording local changes and applying them asynchronously across nodes with built-in loop protection and payload encryption.

---

## 🌟 Key Features

* **Multi-Master / Hub-Spoke Replication**: Independent databases sync bidirectionally without central data lockups.
* **Heterogeneous Engine Support**: Seamlessly replicate between **PostgreSQL** and **MySQL / MariaDB**.
* **Trigger-Based Data Capture**: Changes (`INSERT`, `UPDATE`, `DELETE`) are tracked automatically via low-overhead native DB triggers into a lightweight `sym_change_log` table.
* **Loop Prevention**: Uses session-level bypass mechanisms (`@sym_is_syncing` / `sym.is_syncing`) to prevent recursive sync triggers when applying updates.
* **Composite Primary Key Support**: Handles single and composite primary keys (automatically encoded as JSON arrays).
* **AES-256-GCM Payload Encryption**: Optionally encrypt sensitive change log payloads end-to-end at rest and in transit.
* **Column-Level Filtering**: Restrict synchronized columns on a per-table basis via configuration.
* **Resilient Sync Engine**: Exponential backoff on errors, automated checkpoint recovery, and connection pool management.
* **Health & Monitoring API**: Express-based REST API with rate limiting and key-based authentication for tracking branch replication lags and status.

---

## 📁 Repository Structure

```text
.
├── server.js           # Express API, Hub & Branch engine orchestration, sync workers
├── triggers.js         # Automated schema detection & trigger generator (MySQL & PG)
├── pk-codec.js         # Single & composite primary key decoder utilities
├── range-sync.js       # Range-based manual/batch synchronization utility
├── ping-branches.js    # Health check & ping CLI tool for registered branch DBs
└── package.json
