cat > ~/Documents/Codes/replica/check-env.sh <<'SCRIPT'
#!/usr/bin/env bash
# check-env.sh — validates .env before pm2 restart. Exits non-zero if invalid.
set -uo pipefail
cd "$(dirname "$0")"

node -e '
  require("dotenv").config();

  const errors = [];

  // BRANCHES_JSON
  let branches;
  try {
    branches = JSON.parse(process.env.BRANCHES_JSON || "");
  } catch (e) {
    errors.push("BRANCHES_JSON is not valid JSON: " + e.message);
    branches = [];
  }
  if (branches.length === 0) errors.push("BRANCHES_JSON is empty");
  for (const b of branches) {
    if (!b.id) errors.push("BRANCHES_JSON entry missing id");
    if (!b.url) { errors.push(`BRANCHES_JSON[${b.id}] missing url`); continue; }
    try {
      const u = new URL(b.url);
      if (!u.hostname) errors.push(`BRANCHES_JSON[${b.id}] has empty host`);
      if (u.password && u.password.includes("@")) {
        errors.push(`BRANCHES_JSON[${b.id}] password contains raw @ — URL-encode as %40`);
      }
    } catch (e) {
      errors.push(`BRANCHES_JSON[${b.id}] invalid url: ${e.message}`);
    }
  }

  // LOCAL_URL
  if (!process.env.LOCAL_URL) {
    errors.push("LOCAL_URL is not set");
  } else {
    try {
      const u = new URL(process.env.LOCAL_URL);
      if (u.password && u.password.includes("@")) {
        errors.push("LOCAL_URL password contains raw @ — URL-encode as %40");
      }
    } catch (e) {
      errors.push("LOCAL_URL is invalid: " + e.message);
    }
  }

  // ALLOWED_TABLES
  const tables = (process.env.ALLOWED_TABLES || "")
    .split(",").map(s => s.trim()).filter(Boolean);
  if (tables.length === 0) {
    errors.push("ALLOWED_TABLES is empty");
  }

  // Required secrets
  if (!process.env.API_SECRET_KEY) errors.push("API_SECRET_KEY is not set");

  if (errors.length > 0) {
    console.error(".env INVALID:");
    for (const e of errors) console.error("  - " + e);
    process.exit(1);
  }
  console.log(`.env OK: ${branches.length} branches, ${tables.length} tables`);
'
SCRIPT
chmod +x ~/Documents/Codes/replica/check-env.sh