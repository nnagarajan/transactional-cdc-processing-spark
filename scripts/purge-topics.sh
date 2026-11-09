#!/usr/bin/env bash
#
# Purge every record from the pipeline's four Kafka topics, keeping the topics themselves
# (partitions, configs, and the single-partition metadata topic). Uses kafka-delete-records,
# which moves each partition's log start offset to its current end offset; new records continue
# from there, so offsets are never reused.
#
# Usage: scripts/purge-topics.sh [--yes]
#
# Overridable with environment variables:
#   KAFKA_BOOTSTRAP  bootstrap servers           (default localhost:9092)
#   KAFKA_BIN        directory with the Kafka CLI (default /opt/confluent/confluent-7.7.0/bin)
#   TOPICS           space-separated topic list   (default: the three data topics and the metadata topic)
#
# Preconditions, checked here:
#   - TransactionalCdcProcessingApp and ScdType1MergeApp are stopped
#   - GoldenGate replicats kc and rep3 are stopped (STOP REPLICAT kc / rep3 in GGSCI), so nothing
#     is written to the topics while they are purged
#
# Afterwards:
#   - The app's checkpoint points at offsets that no longer exist. With fail-on-data-loss = true it
#     will fail, so run scripts/reset-pipeline.sh before starting it again.
#   - kc and rep3 resume from their own trail checkpoints and publish only what comes after. To
#     republish older trail data, reposition both to the same trail position first
#     (ALTER REPLICAT ... EXTSEQNO ... EXTRBA ..., see the go-live runbook in gg-configs/README.md).
set -euo pipefail

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/confluent/confluent-7.7.0/bin}"
TOPICS="${TOPICS:-dev.appuser.orders.json dev.appuser.order_details.json dev.appuser.order_line_items.json dev.transaction_metadata_json}"

if pgrep -f "TransactionalCdcProcessingApp|ScdType1MergeApp" > /dev/null; then
  echo "A pipeline app is still running; stop it first:" >&2
  pgrep -af "TransactionalCdcProcessingApp|ScdType1MergeApp" >&2
  exit 1
fi

if pgrep -f "replicat PARAMFILE .*/(kc|rep3)\.prm" > /dev/null; then
  echo "GoldenGate replicat kc or rep3 is still running; stop them in GGSCI first:" >&2
  echo "  STOP REPLICAT kc" >&2
  echo "  STOP REPLICAT rep3" >&2
  exit 1
fi

# Build the offsets JSON: every partition of every topic, deleted up to its current end offset.
json='{"version":1,"partitions":['
sep=''
echo "Records to purge:"
for topic in $TOPICS; do
  while IFS=: read -r t partition end; do
    [[ -z "$end" ]] && continue
    start=$("$KAFKA_BIN/kafka-get-offsets" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$t" --partitions "$partition" --time -2 | cut -d: -f3)
    if (( end > start )); then range="offsets $start..$((end - 1))"; else range="empty"; fi
    printf '  %-40s partition %s: %s record(s) (%s)\n' "$t" "$partition" "$((end - start))" "$range"
    json+="$sep{\"topic\":\"$t\",\"partition\":$partition,\"offset\":$end}"
    sep=','
  done < <("$KAFKA_BIN/kafka-get-offsets" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" --time -1)
done
json+=']}'

if [[ -z "$sep" ]]; then
  echo "No partitions found for: $TOPICS" >&2
  exit 1
fi

if [[ "${1:-}" != "--yes" ]]; then
  read -r -p "Purge these records? [y/N] " answer
  [[ "$answer" == "y" || "$answer" == "Y" ]] || { echo "Aborted."; exit 1; }
fi

offsets_file="$(mktemp)"
trap 'rm -f "$offsets_file"' EXIT
echo "$json" > "$offsets_file"

"$KAFKA_BIN/kafka-delete-records" --bootstrap-server "$KAFKA_BOOTSTRAP" --offset-json-file "$offsets_file"

echo
echo "After purge (topic:partition:log-start-offset should equal the end offset above):"
for topic in $TOPICS; do
  "$KAFKA_BIN/kafka-get-offsets" --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$topic" --time -2
done
echo
echo "Next: scripts/reset-pipeline.sh, then start kc and rep3 (repositioned if needed), then the apps."
