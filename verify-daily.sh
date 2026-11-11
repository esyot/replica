cat > ~/Documents/Codes/replica/verify-daily.sh <<'SCRIPT'
#!/usr/bin/env bash
# verify-daily.sh — runs node-copy --verify once a day, alerts on drift.
set -uo pipefail
cd "$(dirname "$0")"

LOG="verify-daily.log"
TS="$(date '+%Y-%m-%d %H:%M:%S')"

echo "[$TS] Starting verify..." >> "$LOG"
OUTPUT="$(node node-copy.js --verify 2>&1)"
RC=$?

if [ "$RC" -ne 0 ]; then
  echo "[$TS] DRIFT DETECTED (exit $RC)" | tee -a "$LOG"
  echo "$OUTPUT" | tee -a "$LOG"

  if [ -n "${MAILTO:-}" ] && command -v mail >/dev/null 2>&1; then
    echo "$OUTPUT" | mail -s "Replica DRIFT on $(hostname)" "$MAILTO"
  fi
else
  echo "[$TS] OK — all tables in parity" >> "$LOG"
fi

# Keep log under 10MB
if [ -f "$LOG" ] && [ "$(stat -c%s "$LOG" 2>/dev/null || echo 0)" -gt 10485760 ]; then
  tail -1000 "$LOG" > "${LOG}.tmp" && mv "${LOG}.tmp" "$LOG"
fi

exit "$RC"
SCRIPT
chmod +x ~/Documents/Codes/replica/verify-daily.sh