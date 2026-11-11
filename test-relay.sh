cat > ~/Documents/Codes/replica/test-relay.sh <<'SCRIPT'
#!/usr/bin/env bash
# test-relay.sh — insert on branch 02, verify it reaches hub + branch 03.
# Cleans up after itself. Non-zero exit on failure.
set -uo pipefail
cd "$(dirname "$0")"

ID="test-$(date +%s)-$(head -c 6 /dev/urandom | base64 | tr -dc 'a-z0-9')"
echo "Test record_id: $ID"

# Determine a store on branch 02
STORE="$(mysql -h 100.77.79.20 -P 3307 -u mysql -p'candgpos@2026' candg_pos_db -N -e \
  "SELECT record_id FROM store LIMIT 1;" 2>/dev/null)"
if [ -z "$STORE" ]; then
  echo "FAIL: could not read store from branch 02"
  exit 1
fi

echo "→ Inserting on branch 02 (store=$STORE)..."
mysql -h 100.77.79.20 -P 3307 -u mysql -p'candgpos@2026' candg_pos_db -e "
  INSERT INTO pos_session
    (record_id, user, store, opening_balance, opened_at, status, created_at, updated_at)
  VALUES
    ('$ID', NULL, '$STORE', 0.00, NOW(), '1', NOW(), NOW());
" 2>/dev/null || { echo "FAIL: insert failed"; exit 1; }

echo "→ Waiting 8s for propagation..."
sleep 8

FAIL=0
BR02=$(mysql -h 100.77.79.20 -P 3307 -u mysql -p'candgpos@2026' candg_pos_db -N -e \
  "SELECT COUNT(*) FROM pos_session WHERE record_id='$ID';" 2>/dev/null)
HUB=$(mysql -h 127.0.0.1 -P 3306 -u root -p'candgtrading@2026' candg_pos_db -N -e \
  "SELECT COUNT(*) FROM pos_session WHERE record_id='$ID';" 2>/dev/null)
BR03=$(mysql -h 100.121.191.92 -P 3307 -u mysql -p'candgpos@2026' candg_pos_db -N -e \
  "SELECT COUNT(*) FROM pos_session WHERE record_id='$ID';" 2>/dev/null)

[ "$BR02" = "1" ] && echo "  ✅ BR02" || { echo "  ❌ BR02 (found $BR02)"; FAIL=1; }
[ "$HUB"  = "1" ] && echo "  ✅ HUB " || { echo "  ❌ HUB  (found $HUB)";  FAIL=1; }
[ "$BR03" = "1" ] && echo "  ✅ BR03" || { echo "  ❌ BR03 (found $BR03)"; FAIL=1; }

echo "→ Cleaning up..."
mysql -h 100.77.79.20 -P 3307 -u mysql -p'candgpos@2026' candg_pos_db -e \
  "DELETE FROM pos_session WHERE record_id='$ID';" 2>/dev/null

sleep 6

# Verify cleanup propagated
BR03_DEL=$(mysql -h 100.121.191.92 -P 3307 -u mysql -p'candgpos@2026' candg_pos_db -N -e \
  "SELECT COUNT(*) FROM pos_session WHERE record_id='$ID';" 2>/dev/null)
[ "$BR03_DEL" = "0" ] && echo "  ✅ cleanup propagated" || { echo "  ⚠️  cleanup left $BR03_DEL on BR03"; FAIL=1; }

echo ""
if [ "$FAIL" = "0" ]; then
  echo "✅ RELAY WORKING"
  exit 0
else
  echo "❌ RELAY BROKEN"
  exit 1
fi
SCRIPT
chmod +x ~/Documents/Codes/replica/test-relay.sh