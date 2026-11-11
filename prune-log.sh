cat > ~/Documents/Codes/replica/prune-log.sh <<'SCRIPT'
#!/usr/bin/env bash
# prune-log.sh — delete sym_change_log rows older than N days (default 90).
set -uo pipefail
cd "$(dirname "$0")"

DAYS="${PRUNE_DAYS:-90}"

RESULT=$(mysql -h 127.0.0.1 -P 3306 -u root -p'candgtrading@2026' candg_pos_db -N -e "
  DELETE FROM sym_change_log WHERE created_at < NOW() - INTERVAL $DAYS DAY;
  SELECT ROW_COUNT() AS pruned;
" 2>/dev/null)

echo "[$(date '+%Y-%m-%d %H:%M:%S')] pruned $RESULT row(s) (older than $DAYS days)" >> prune-log.log
SCRIPT
chmod +x ~/Documents/Codes/replica/prune-log.sh