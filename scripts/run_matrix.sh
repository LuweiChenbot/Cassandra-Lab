#!/usr/bin/env bash
# Run the full matrix for one scenario: RYW, MR, MW and WFR, each with W/R =
# ONE/ONE, ONE/QUORUM, QUORUM/QUORUM and ALL/ONE. Node roles and setup/verify
# CLs are chosen per scenario so no unreachable node is used.
#
# Usage: ./scripts/run_matrix.sh <scenario> [iterations=1000]
#   normal              all nodes up (run inject_latency.sh first)
#   node_failure        after: docker stop cass3
#   partition_majority  after: ./scripts/partition.sh cass3 (uses cass1, cass2)
#   partition_minority  after: ./scripts/partition.sh cass3 (uses cass3 only)

set -uo pipefail
cd "$(dirname "$0")/.."

SCENARIO="${1:-normal}"
N="${2:-1000}"
PY=./.venv/bin/python

case "$SCENARIO" in
  normal)
    NODES="1,2,3"
    RYW="--write-node 1 --read-node 2"
    MR="--write-node 1 --read1-node 1 --read2-node 2 --seed-cl ALL"
    MW="--write1-node 2 --write2-node 3 --verify-node 1 --verify-cl ALL"
    WFR="--write-node 1 --read-node 2 --client-node 3 --verify-node 1 --setup-cl ALL --verify-cl ALL"
    ;;
  node_failure|partition_majority)
    # only cass1 and cass2 reachable: setup/verify CLs drop to QUORUM
    NODES="1,2"
    RYW="--write-node 1 --read-node 2"
    MR="--write-node 1 --read1-node 1 --read2-node 2 --seed-cl QUORUM"
    # QUORUM verification needs --settle-ms, or replication lag looks like misordering
    MW="--write1-node 1 --write2-node 2 --verify-node 1 --verify-cl QUORUM --settle-ms 1000"
    WFR="--write-node 1 --read-node 2 --client-node 1 --verify-node 1 --setup-cl QUORUM --verify-cl QUORUM --settle-ms 1000"
    ;;
  partition_minority)
    # only cass3 reachable: setup CLs drop to ONE. Everything runs on one node,
    # so this measures availability, not violations.
    NODES="3"
    RYW="--write-node 3 --read-node 3"
    MR="--write-node 3 --read1-node 3 --read2-node 3 --seed-cl ONE"
    MW="--write1-node 3 --write2-node 3 --verify-node 3 --verify-cl ONE --settle-ms 1000"
    WFR="--write-node 3 --read-node 3 --client-node 3 --verify-node 3 --setup-cl ONE --verify-cl ONE --settle-ms 1000"
    ;;
  *)
    echo "unknown scenario: $SCENARIO" >&2
    echo "choose from: normal | node_failure | partition_majority | partition_minority" >&2
    exit 1
    ;;
esac

# both partition sides are tagged "partition" in the CSV
case "$SCENARIO" in
  partition_*) TAG="partition" ;;
  *)           TAG="$SCENARIO" ;;
esac

# the four W/R combinations
COMBOS=("ONE ONE" "ONE QUORUM" "QUORUM QUORUM" "ALL ONE")

echo "==================================================================="
echo " matrix  scenario=$SCENARIO  tag=$TAG  nodes=$NODES  n=$N"
echo "==================================================================="

run_one() {
  local model="$1" extra="$2" w="$3" r="$4"
  echo
  echo "-------------------------------------------------------------------"
  echo ">>> $model   W=$w  R=$r"
  echo "-------------------------------------------------------------------"
  # shellcheck disable=SC2086
  $PY "experiments/${model}_test.py" \
      --scenario "$TAG" --nodes "$NODES" \
      --write-cl "$w" --read-cl "$r" -n "$N" $extra \
    || echo "    [exited non-zero, continuing]"
}

for combo in "${COMBOS[@]}"; do
  w="${combo% *}"; r="${combo#* }"
  run_one ryw "$RYW" "$w" "$r"
  run_one mr  "$MR"  "$w" "$r"
  run_one mw  "$MW"  "$w" "$r"
  run_one wfr "$WFR" "$w" "$r"
done

echo
echo "==================================================================="
echo " matrix done. Summary: results/all_results.csv"
echo " per-iteration traces: results/traces/"
echo "==================================================================="
