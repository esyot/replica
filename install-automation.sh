cat > ~/Documents/Codes/replica/install-automation.sh <<'SCRIPT'
#!/usr/bin/env bash
# install-automation.sh — adds cron entries for healthcheck, verify, backup,
# prune, morning-report. Idempotent (won't duplicate entries).
set -euo pipefail
cd "$(dirname "$0")"
DIR="$(pwd)"

# Remove existing replica cron entries (any line containing this dir)
TMP="$(mktemp)"
crontab -l 2>/dev/null | grep -v "$DIR" > "$TMP" || true

# Add fresh entries
cat >> "$TMP" <<CRON
# Replica automation — installed $(date '+%Y-%m-%d %H:%M:%S')
*/15 * * * * $DIR/healthcheck.sh >/dev/null 2>&1
0 3 * * *   $DIR/verify-daily.sh >/dev/null 2>&1
5 3 * * *   $DIR/backup-daily.sh >/dev/null 2>&1
0 4 1 * *   $DIR/prune-log.sh >/dev/null 2>&1
0 7 * * *   $DIR/morning-report.sh >/dev/null 2>&1
CRON

crontab "$TMP"
rm -f "$TMP"

echo "Cron jobs installed. Current replica-related crons:"
crontab -l | grep "$DIR"
SCRIPT
chmod +x ~/Documents/Codes/replica/install-automation.sh