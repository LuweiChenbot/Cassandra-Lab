#!/usr/bin/env bash
# Partition one node from the others: drop inter-node traffic (port 7000) in
# both directions. Its client port stays reachable, so clients can use either
# side. Needs the qdisc set up by inject_latency.sh.
#
# Usage: ./scripts/partition.sh cass3      Undo: ./scripts/heal_partition.sh

set -euo pipefail

TARGET="${1:-cass3}"
ALL_NODES="${NODES:-cass1 cass2 cass3}"
STORAGE_PORT=7000

ip_of() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"
}

# requires the prio qdisc from inject_latency.sh
if ! docker exec "$TARGET" tc qdisc show dev eth0 | grep -q 'prio 1:'; then
  echo "error: $TARGET has no tc qdisc. Run ./scripts/inject_latency.sh first" >&2
  exit 1
fi

OTHERS=""
for c in $ALL_NODES; do [ "$c" = "$TARGET" ] || OTHERS="$OTHERS $c"; done

TARGET_IP=$(ip_of "$TARGET")
echo "Isolating $TARGET ($TARGET_IP) from [$OTHERS ] on :$STORAGE_PORT"
echo

# target -> peers
for c in $OTHERS; do
  peer_ip=$(ip_of "$c")
  docker exec "$TARGET" tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 \
      match ip dst "$peer_ip"/32 match ip dport $STORAGE_PORT 0xffff flowid 1:3
  echo "  [$TARGET] drop -> $c ($peer_ip):$STORAGE_PORT"
done

# peers -> target (without this the partition would be one-way)
for c in $OTHERS; do
  docker exec "$c" tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 \
      match ip dst "$TARGET_IP"/32 match ip dport $STORAGE_PORT 0xffff flowid 1:3
  echo "  [$c] drop -> $TARGET ($TARGET_IP):$STORAGE_PORT"
done

echo
echo "Waiting for gossip to converge (~20 s) ..."
sleep 20
echo
for c in $ALL_NODES; do
  echo "--- ring as seen by $c ---"
  docker exec "$c" nodetool status 2>/dev/null | grep -E '^[UD][NLJM]' | sed 's/^/    /' \
    || echo "    (unavailable)"
done
echo
echo "majority: [$OTHERS ]  ->  use --nodes without ${TARGET#cass}"
echo "minority: $TARGET      ->  use --nodes ${TARGET#cass}"
