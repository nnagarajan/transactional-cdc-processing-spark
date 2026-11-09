-- Create Delta Lake table for dropped_events: one row per input the pipeline discarded
-- (replayed or duplicate events and markers, unknown tables, purged buffers without a marker,
-- unparseable Kafka messages), with the reason.
--
-- No DROP TABLE here: this is an audit trail, and re-running SeedJob must not erase it.

CREATE TABLE IF NOT EXISTS {{CATALOG_SCHEMA}}.dropped_events (
    -- What was dropped, and why
    droppedTs TIMESTAMP COMMENT 'When the micro-batch that dropped it was written',
    droppedDate DATE COMMENT 'Partition column: date of droppedTs',
    batchId BIGINT COMMENT 'Micro-batch id that dropped it',
    itemKind STRING COMMENT 'DATA_EVENT | MARKER | PURGED_BUFFER | RAW_MESSAGE',
    reason STRING COMMENT 'REPLAY_BELOW_CURSOR | REPLAY_ALREADY_EMITTED | DUPLICATE_MARKER | DUPLICATE_EVENT | UNKNOWN_TABLE | NO_MARKER_BELOW_CURSOR | MISSING_IDS | UNPARSEABLE',
    droppedCount INT COMMENT '1 for a full row, or the number of items folded into a summary row',
    lastEmittedCsn STRING COMMENT 'Last emitted CSN (the cursor) at the time of the drop',

    -- Transaction identity (null for UNPARSEABLE)
    xid STRING COMMENT 'Oracle transaction ID',
    csn STRING COMMENT 'Oracle commit sequence number',

    -- Where it came from in Kafka
    kafkaTopic STRING COMMENT 'Source topic',
    kafkaPartition INT COMMENT 'Source partition',
    kafkaOffset BIGINT COMMENT 'Source offset',
    markerIndex INT COMMENT 'Position of the marker within a grouped metadata message',

    -- DATA_EVENT
    tableName STRING COMMENT 'Source table of the event',
    opType STRING COMMENT 'I | U | D',
    opTs STRING COMMENT 'Operation timestamp',
    pos STRING COMMENT 'GoldenGate trail position',
    before MAP<STRING, STRING> COMMENT 'Before image',
    after MAP<STRING, STRING> COMMENT 'After image',

    -- MARKER
    markerEventCount INT COMMENT 'Total event count declared by the marker',
    markerTxTs STRING COMMENT 'Transaction timestamp declared by the marker',
    dataCollections ARRAY<STRUCT<dataCollection: STRING, eventCount: INT>> COMMENT 'Per-table counts declared by the marker',

    -- PURGED_BUFFER
    bufferedOrders INT COMMENT 'ORDERS events buffered when the buffer was removed',
    bufferedDetails INT COMMENT 'ORDER_DETAILS events buffered when the buffer was removed',
    bufferedLineItems INT COMMENT 'ORDER_LINE_ITEMS events buffered when the buffer was removed',

    -- RAW_MESSAGE
    rawValue STRING COMMENT 'Kafka message value that could not be used'
)
USING DELTA
PARTITIONED BY (droppedDate)
COMMENT 'Inputs discarded by TransactionalCdcProcessingApp, with the reason'
TBLPROPERTIES (
    'delta.feature.catalogManaged' = 'supported',
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
