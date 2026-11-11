cat > ~/Documents/Codes/replica/healthcheck.sh <<'SCRIPT'
#!/usr/bin/env bash
# healthcheck.sh — hits /health, logs failures, returns non-zero on unhealthy.
# Intended for cron: */15 * * * * /path/healthcheck.sh
set -uo pipefail
cd "$(dirname "$0")"

API_KEY="$(grep '^API_SECRET_KEY=' .env | cut -d'=' -f2- | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")"
URL="${HEALTH_URL:-http://localhost:8182/health}"
LOG="healthcheck.log"
TS="$(date '+%Y-%m-%d %H:%M:%S')"

RESP="$(curl -s -m 10 -H "x-api-key: $API_KEY" "$URL" 2>&1)" || {
  echo "[$TS] ALERT: /health unreachable: $RESP" | tee -a "$LOG"
  exit 1
}

HEALTHY="$(echo "$RESP" | jq -r '.healthy // false' 2>/dev/null)"
if [ "$HEALTHY" = "true" ]; then
  # Log only at :00 to avoid noise
  [ "$(date +%M)" = "00" ] && echo "[$TS] OK" >> "$LOG"
  exit 0
fi

SUMMARY="$(echo "$RESP" | jq -r '
  "healthy=\(.healthy) anyHalted=\(.anyHalted)\n" +
  ([.branches[]? | "  \(.branchId): healthy=\(.healthy) errors=\(.consecutiveErrors // 0) status=\(.status // "?")"] | join("\n"))
' 2>/dev/null || echo "$RESP")"

echo "[$TS] ALERT: unhealthy" | tee -a "$LOG"
echo "$SUMMARY" | tee -a "$LOG"

if [ -n "${MAILTO:-}" ] && command -v mail >/dev/null 2>&1; then
  echo "$SUMMARY" | mail -s "Replica health ALERT on $(hostname)" "$MAILTO"
fi

if [ -n "${DISPLAY:-}" ] && command -v notify-send >/dev/null 2>&1; then
  notify-send -u critical "Replica UNHEALTHY" "$SUMMARY"
fi

exit 1
SCRIPT
chmod +x ~/Documents/Codes/replica/healthcheck.sh