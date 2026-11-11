cat > ~/Documents/Codes/replica/backup-daily.sh <<'SCRIPT'
#!/usr/bin/env bash
# backup-daily.sh — mysqldump of all ALLOWED_TABLES on the hub.
set -uo pipefail
cd "$(dirname "$0")"

TS="$(date +%Y%m%d-%H%M%S)"
mkdir -p backups
OUT="backups/hub-${TS}.sql"
LOG="backup-daily.log"

TABLES="$(grep '^ALLOWED_TABLES=' .env | cut -d'=' -f2- | sed -e 's/^"//' -e 's/"$//')"

# Convert comma list to space list for mysqldump
TABLES_SPACE="$(echo "$TABLES" | tr ',' ' ')"

mysqldump -h 127.0.0.1 -P 3306 -u root -p'candgtrading@2026' \
  --single-transaction --no-create-info --complete-insert \
  candg_pos_db $TABLES_SPACE > "$OUT" 2>/dev/null

RC=$?
if [ "$RC" -ne 0 ] || [ ! -s "$OUT" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FAILED (exit $RC)" >> "$LOG"
  rm -f "$OUT"
  exit 1
fi

SIZE=$(stat -c%s "$OUT" 2>/dev/null || stat -f%z "$OUT")
echo "[$(date '+%Y-%m-%d %H:%M:%S')] OK: $OUT ($SIZE bytes)" >> "$LOG"

# Keep 30 days
ls -t backups/hub-*.sql 2>/dev/null | tail -n +31 | xargs -r rm -f
SCRIPT
chmod +x ~/Documents/Codes/replica/backup-daily.sh