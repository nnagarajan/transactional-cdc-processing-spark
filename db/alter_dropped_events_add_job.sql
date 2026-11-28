-- Adds the columns that configurable job models write to an existing dropped_events table:
-- the job that dropped each item, and per-table buffered counts for PURGED_BUFFER rows.
-- Run once, with both apps stopped, before deploying the version with job models:
--
--   spark-sql ... -f db/alter_dropped_events_add_job.sql   (replace {{CATALOG_SCHEMA}} first)
--
-- New installs get the columns from create_dropped_events_table.deltalake.sql. Rows written
-- before the change keep jobName null and their counts in bufferedOrders, bufferedDetails and
-- bufferedLineItems. Not named *.deltalake.sql, so SeedJob does not run it.

ALTER TABLE {{CATALOG_SCHEMA}}.dropped_events ADD COLUMNS (
    jobName STRING COMMENT 'Job that dropped the item (jobs/<job>.conf job.name)',
    bufferedCounts ARRAY<STRUCT<dataCollection: STRING, eventCount: INT>> COMMENT 'PURGED_BUFFER: events buffered per tracked table when the buffer was removed'
);
