#!/usr/bin/env bash
# Runs CdcModelTool on a job config, without Spark or Oracle:
#
#   scripts/cdc-model.sh ddl jobs/orders.conf [dir]   write db/<job>_stream.deltalake.sql and
#                                                     db/<job>_current.deltalake.sql (dir: db)
#   scripts/cdc-model.sh check jobs/orders.conf       validate the job config, listing every problem
#
# Builds the classes first if needed. The test classpath is used because it is the one Maven
# scope holding Spark's runtime dependencies (Spark is `provided`).
set -euo pipefail
cd "$(dirname "$0")/.."
./mvnw -q compile exec:exec -Dexec.classpathScope=test \
  -Dexec.args="-classpath %classpath com.technext.demos.txbuffering.CdcModelTool $*" 2>&1 \
  | grep -v "^Using Spark's default log4j profile"
