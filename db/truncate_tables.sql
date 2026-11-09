-- Empty the pipeline's three tables, keeping their definitions (schema, partitioning, properties).
--
-- Run with both apps stopped, then delete both checkpoints and restart: see scripts/reset-pipeline.sh,
-- which does all of that. Not named *.deltalake.sql, so SeedJob never runs it.
--
-- DELETE FROM rather than TRUNCATE TABLE: it is a normal Delta commit, which Unity Catalog managed
-- tables accept, and it keeps the history (time travel) until VACUUM removes the old files.

DELETE FROM mymanaged.argus1.order_stream;
DELETE FROM mymanaged.argus1.orders_current;
DELETE FROM mymanaged.argus1.dropped_events;

SELECT 'order_stream' AS table_name, count(*) AS row_count FROM mymanaged.argus1.order_stream
UNION ALL SELECT 'orders_current', count(*) FROM mymanaged.argus1.orders_current
UNION ALL SELECT 'dropped_events', count(*) FROM mymanaged.argus1.dropped_events;
