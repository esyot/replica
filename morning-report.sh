cat > ~/Documents/Codes/replica/morning-report.sh <<'SCRIPT'
#!/usr/bin/env bash
# morning-report.sh — summary for the operator. Run daily at 07:00.
set -uo pipefail
cd "$(dirname "$0")"

REPORT="morning-report.txt"
API_KEY="$(grep '^API_SECRET_KEY=' .env | cut -d'=' -f2- | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")"

{
  echo "════════════════════════════════════════════════════"
  echo "Replica morning report — $(date)"
  echo "════════════════════════════════════════════════════"
  echo ""

  echo "▸ ENGINE"
  pm2 status replica-engine 2>/dev/null | grep replica-engine || echo "  not running"
  echo ""

  echo "▸ HEALTH"
  RESP="$(curl -s -m 5 -H "x-api-key: $API_KEY" http://localhost:8182/health 2>&1)"
  if [ -z "$RESP" ] || echo "$RESP" | jq -e '.error' >/dev/null 2>&1; then
    echo "  ⚠️  /health unreachable or unauthorized"
  else
    echo "$RESP" | jq -r '
      "  healthy: \(.healthy)",
      "  any halted: \(.anyHalted)",
      "  deferred rows: \(.totalDeferredRows // 0)",
      "  relay enabled: \(.relayEnabled // "?")",
      "",
      "  Branches:",
      (.branches[]? | "    \(.branchId): healthy=\(.healthy) status=\(.status // "?") errors=\(.consecutiveErrors // 0)")
    ' 2>/dev/null
  fi
  echo ""

  echo "▸ HEALTHCHECK (last 3)"
  tail -3 healthcheck.log 2>/dev/null | sed 's/^/  /' || echo "  (none)"
  echo ""

  echo "▸ VERIFY-DAILY (last 3)"
  tail -3 verify-daily.log 2>/dev/null | sed 's/^/  /' || echo "  (none)"
  echo ""

  echo "▸ BACKUP (last 3)"
  tail -3 backup-daily.log 2>/dev/null | sed 's/^/  /' || echo "  (none)"
  echo ""

  echo "▸ POS_SESSION"
  for spec in "127.0.0.1 3306 root candgtrading@2026 HUB" "100.77.79.20 3307 mysql candgpos@2026 BR02" "100.121.191.92 3307 mysql candgpos@2026 BR03"; do
    set -- $spec
    N=$(mysql -h "$1" -P "$2" -u "$3" -p"$4" candg_pos_db -N -e "SELECT COUNT(*) FROM pos_session;" 2>/dev/null)
    echo "  $5: ${N:-?} rows"
  done
} > "$REPORT"

cat "$REPORT"
SCRIPT
chmod +x ~/Documents/Codes/replica/morning-report.sh