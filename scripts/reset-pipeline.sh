#!/usr/bin/env bash
#
# Reset both pipelines to reprocess from the start of the Kafka topics:
#   1. refuse to run while TransactionalCdcProcessingApp or ScdType1MergeApp is running
#   2. empty order_stream, orders_current and dropped_events (db/truncate_tables.sql)
#   3. delete both checkpoints, so the sequencer state and Kafka/Delta offsets start fresh
#
# Usage: scripts/reset-pipeline.sh [--yes]
#
# Overridable with environment variables:
#   CDC_CHECKPOINT  checkpoint of TransactionalCdcProcessingApp (default /tmp/spark-checkpoints-ordered)
#   SCD_CHECKPOINT  checkpoint of ScdType1MergeApp            (default /tmp/spark-checkpoints-scd)
#   UC_URI          Unity Catalog server                      (default http://localhost:7071)
#
# After the reset, start TransactionalCdcProcessingApp first, then ScdType1MergeApp. With a new
# checkpoint the main app reads every topic from kafka.starting-offsets (default earliest); remove
# any job.kafka.starting-offsets.<topic-key> overrides first if you want the full topics.
set -euo pipefail

cd "$(dirname "$0")/.."

CDC_CHECKPOINT="${CDC_CHECKPOINT:-/tmp/spark-checkpoints-ordered}"
SCD_CHECKPOINT="${SCD_CHECKPOINT:-/tmp/spark-checkpoints-scd}"
UC_URI="${UC_URI:-http://localhost:7071}"
PACKAGES="io.delta:delta-spark_4.1_2.13:4.2.0,io.unitycatalog:unitycatalog-spark_4.1_2.13:0.5.0"

if pgrep -f "TransactionalCdcProcessingApp|ScdType1MergeApp" > /dev/null; then
  echo "A pipeline app is still running; stop it first:" >&2
  pgrep -af "TransactionalCdcProcessingApp|ScdType1MergeApp" >&2
  exit 1
fi

if ! curl -sf -m 5 "$UC_URI/api/2.1/unity-catalog/catalogs" > /dev/null; then
  echo "Unity Catalog server is not reachable at $UC_URI; start it first (bin/start-uc-server -p 7071)." >&2
  exit 1
fi

echo "This will:"
echo "  - delete all rows from mymanaged.argus1.order_stream, orders_current and dropped_events"
echo "  - delete checkpoint $CDC_CHECKPOINT"
echo "  - delete checkpoint $SCD_CHECKPOINT"
if [[ "${1:-}" != "--yes" ]]; then
  read -r -p "Continue? [y/N] " answer
  [[ "$answer" == "y" || "$answer" == "Y" ]] || { echo "Aborted."; exit 1; }
fi

echo "Emptying tables..."
spark-sql --master 'local[1]' \
  --packages "$PACKAGES" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.mymanaged=io.unitycatalog.spark.UCSingleCatalog \
  --conf "spark.sql.catalog.mymanaged.uri=$UC_URI" \
  --conf spark.sql.catalog.mymanaged.auth.type=static \
  --conf spark.sql.catalog.mymanaged.auth.token=dummy \
  --conf spark.sql.defaultCatalog=mymanaged \
  -f db/truncate_tables.sql 2> /tmp/reset-pipeline-spark-sql.log \
  || { echo "spark-sql failed; see /tmp/reset-pipeline-spark-sql.log" >&2; exit 1; }

echo "Deleting checkpoints..."
for cp in "$CDC_CHECKPOINT" "$SCD_CHECKPOINT"; do
  case "$cp" in
    ""|"/"|"/tmp"|"/tmp/") echo "Refusing to delete '$cp'" >&2; exit 1 ;;
  esac
  rm -rf -- "$cp"
  echo "  removed $cp"
done

echo
echo "Done. Next:"
echo "  1. start TransactionalCdcProcessingApp (reads the topics from kafka.starting-offsets)"
echo "  2. start ScdType1MergeApp"
