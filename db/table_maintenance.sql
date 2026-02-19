-- Table maintenance operations for Delta Lake tables

-- ============================================================================
-- VACUUM - Remove old data files
-- ============================================================================
-- By default, Delta Lake keeps 7 days of history
-- VACUUM removes files older than the retention period

-- Check current retention settings
DESCRIBE DETAIL order_stream;

-- Dry run to see what files would be deleted (don't actually delete)
VACUUM order_stream RETAIN 168 HOURS DRY RUN;

-- Actually vacuum (retain 7 days = 168 hours)
VACUUM order_stream RETAIN 168 HOURS;

-- Vacuum with shorter retention (use with caution!)
-- Note: Set delta.deletedFileRetentionDuration if needed
-- VACUUM order_stream RETAIN 24 HOURS;


-- ============================================================================
-- OPTIMIZE - Compact small files and improve query performance
-- ============================================================================

-- Optimize the entire table
OPTIMIZE order_stream;

-- Optimize with Z-ordering on frequently filtered columns
OPTIMIZE order_stream ZORDER BY (orderId, xid);

-- ============================================================================
-- ANALYZE - Update table statistics for better query planning
-- ============================================================================

-- Compute statistics for the table
ANALYZE TABLE order_stream COMPUTE STATISTICS;

-- Compute statistics for specific columns
ANALYZE TABLE order_stream COMPUTE STATISTICS FOR COLUMNS orderId, xid, csn;


-- ============================================================================
-- DESCRIBE - View table metadata and history
-- ============================================================================

-- Show table schema
DESCRIBE order_stream;

-- Show extended table information
DESCRIBE EXTENDED order_stream;

-- Show detailed table information
DESCRIBE DETAIL order_stream;

-- Show table history (versions)
DESCRIBE HISTORY order_stream;

-- Show history for specific number of versions
DESCRIBE HISTORY order_stream LIMIT 20;


-- ============================================================================
-- DATA SKIPPING - Configure data skipping for better performance
-- ============================================================================

-- Enable auto-compaction and optimize write
ALTER TABLE order_stream SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Configure data skipping statistics
ALTER TABLE order_stream SET TBLPROPERTIES (
    'delta.dataSkippingNumIndexedCols' = '10'
);


-- ============================================================================
-- CLONE - Create table copies (shallow or deep)
-- ============================================================================

-- Create a shallow clone (references same data files)
CREATE TABLE order_stream_clone
SHALLOW CLONE order_stream;

-- Create a deep clone (copies all data)
-- CREATE TABLE order_stream_backup
-- DEEP CLONE order_stream;


-- ============================================================================
-- RESTORE - Restore table to previous version
-- ============================================================================

-- Restore to specific version
-- RESTORE TABLE order_stream TO VERSION AS OF 5;

-- Restore to specific timestamp
-- RESTORE TABLE order_stream TO TIMESTAMP AS OF '2026-02-16 10:00:00';


-- ============================================================================
-- FILE MANAGEMENT
-- ============================================================================

-- Show records in the Delta table
SELECT * FROM delta.`/tmp/delta-tables/order_stream`;

-- Count number of records
SELECT COUNT(*) as num_records
FROM delta.`/tmp/delta-tables/order_stream`;


-- ============================================================================
-- PERFORMANCE MONITORING
-- ============================================================================

-- Check table size
SELECT
    numFiles as number_of_files,
    sizeInBytes / (1024*1024) as size_mb,
    numFiles / GREATEST(1, sizeInBytes / (1024*1024*128)) as avg_file_size_mb
FROM (DESCRIBE DETAIL order_stream);

-- Check version count
SELECT COUNT(*) as version_count
FROM (DESCRIBE HISTORY order_stream);
