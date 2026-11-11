#!/usr/bin/env bash
# Trigger audit — loops all three nodes, lists every sym_trig_* trigger.
# Flags tables that don't have exactly 3 triggers (ins/upd/del).
set -uo pipefail
cd ~/Documents/Codes/replica

# Load API list of expected tables from .env
TABLES="$(grep '^ALLOWED_TABLES=' .env | cut -d'=' -f2- | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//" | tr ',' '\n' | sed 's/^ *//; s/ *$//' | grep -v '^$')"
TOTAL=$(echo "$TABLES" | wc -l)
echo "Expected tables: $TOTAL"
echo ""

# Node spec: host port user password label
NODES=(
  "127.0.0.1 3306 root candgtrading@2026 HUB"
  "100.77.79.20 3307 mysql candgpos@2026 BR02"
  "100.121.191.92 3307 mysql candgpos@2026 BR03"
)

for spec in "${NODES[@]}"; do
  set -- $spec
  HOST=$1; PORT=$2; USER=$3; PASS=$4; LABEL=$5

  echo "=================================================="
  echo "=== $LABEL ($HOST:$PORT) ==="
  echo "=================================================="

  # Total trigger count on this node
  TOTAL_TRIGS=$(mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$PASS" candg_pos_db -N -e "
    SELECT COUNT(*) FROM information_schema.TRIGGERS
    WHERE TRIGGER_SCHEMA=DATABASE() AND TRIGGER_NAME LIKE 'sym_trig_%';
  " 2>/dev/null)
  echo "Total sym_trig_* triggers: $TOTAL_TRIGS (expected: $((TOTAL * 3)))"
  echo ""

  # Per-table breakdown: how many triggers per table
  echo "--- Per-table trigger counts (ins/upd/del) ---"
  mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$PASS" candg_pos_db -e "
    SELECT
      REGEXP_REPLACE(TRIGGER_NAME, '^sym_trig_(.*)_(ins|upd|del)$', '\\1') AS table_name,
      COUNT(*) AS triggers,
      GROUP_CONCAT(
        CASE EVENT_MANIPULATION
          WHEN 'INSERT' THEN 'ins'
          WHEN 'UPDATE' THEN 'upd'
          WHEN 'DELETE' THEN 'del'
          ELSE EVENT_MANIPULATION
        END ORDER BY EVENT_MANIPULATION
      ) AS has
    FROM information_schema.TRIGGERS
    WHERE TRIGGER_SCHEMA=DATABASE() AND TRIGGER_NAME LIKE 'sym_trig_%'
    GROUP BY table_name
    ORDER BY triggers ASC, table_name;
  " 2>/dev/null

  # Cross-check: which expected tables have less than 3 triggers?
  echo ""
  echo "--- Missing or incomplete tables ---"
  MISSING=0
  for table in $TABLES; do
    COUNT=$(mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$PASS" candg_pos_db -N -e "
      SELECT COUNT(*) FROM information_schema.TRIGGERS
      WHERE TRIGGER_SCHEMA=DATABASE() AND TRIGGER_NAME LIKE 'sym_trig_${table}_%';
    " 2>/dev/null)
    if [ "$COUNT" != "3" ]; then
      echo "  ⚠️  $table: $COUNT trigger(s) (expected 3)"
      MISSING=$((MISSING + 1))
    fi
  done
  if [ "$MISSING" = "0" ]; then
    echo "  ✅ All $TOTAL tables have 3 triggers each"
  else
    echo "  ❌ $MISSING table(s) with incorrect trigger count"
  fi

  # Bonus: tables that HAVE triggers but aren't in ALLOWED_TABLES
  echo ""
  echo "--- Orphan triggers (exist but table not in ALLOWED_TABLES) ---"
  ORPHANS=$(mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$PASS" candg_pos_db -N -e "
    SELECT DISTINCT REGEXP_REPLACE(TRIGGER_NAME, '^sym_trig_(.*)_(ins|upd|del)$', '\\1') AS t
    FROM information_schema.TRIGGERS
    WHERE TRIGGER_SCHEMA=DATABASE() AND TRIGGER_NAME LIKE 'sym_trig_%';
  " 2>/dev/null)
  ORPHAN_COUNT=0
  for t in $ORPHANS; do
    if ! echo "$TABLES" | grep -qx "$t"; then
      echo "  ⚠️  $t (trigger exists, not in ALLOWED_TABLES)"
      ORPHAN_COUNT=$((ORPHAN_COUNT + 1))
    fi
  done
  if [ "$ORPHAN_COUNT" = "0" ]; then
    echo "  ✅ No orphan triggers"
  fi

  echo ""
done