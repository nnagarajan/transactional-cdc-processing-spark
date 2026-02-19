-- Schema evolution examples for Delta Lake tables
-- Delta Lake supports schema evolution including adding columns, changing nullability, etc.

-- ============================================================================
-- ADD COLUMNS
-- ============================================================================

-- Add a new top-level column
ALTER TABLE order_stream
ADD COLUMNS (
    processing_timestamp TIMESTAMP COMMENT 'Timestamp when record was processed'
);

-- Add a new column with default value (Delta Lake 2.3+)
ALTER TABLE order_stream
ADD COLUMNS (
    data_version STRING DEFAULT '1.0' COMMENT 'Data schema version'
);


-- ============================================================================
-- CHANGE COLUMN METADATA
-- ============================================================================

-- Change column comment
ALTER TABLE order_stream
ALTER COLUMN orderId COMMENT 'Unique identifier for the order (primary key)';

-- Rename column (Delta Lake 1.2+)
ALTER TABLE order_stream
RENAME COLUMN processing_timestamp TO ingestion_timestamp;


-- ============================================================================
-- DROP COLUMNS (Delta Lake 2.3+)
-- ============================================================================

-- Drop a column
-- Note: This only removes the column from the schema, not from the underlying Parquet files
-- ALTER TABLE order_stream DROP COLUMN data_version;


-- ============================================================================
-- CHANGE DATA TYPE
-- ============================================================================

-- Note: Direct type changes are limited in Delta Lake
-- For complex type changes, create a new table with the desired schema

-- Example: Change orderId from DOUBLE to DECIMAL
-- Step 1: Create new table with desired schema
CREATE TABLE order_stream_v2 (
    orderId DECIMAL(20,0) COMMENT 'Primary order identifier',
    xid STRING COMMENT 'Oracle transaction ID (xid)',
    csn STRING COMMENT 'Oracle commit sequence number',
    dwhProcessedTs STRING,
    orders ARRAY<STRUCT<
        orderId: DECIMAL(20,0),
        orderRef: STRING,
        version: DECIMAL(10,0),
        orderDate: STRING,
        orderTs: STRING,
        orderStatus: STRING,
        orderType: STRING,
        totalAmount: DECIMAL(20,4),
        currency: STRING,
        customerId: STRING,
        shippingAddressId: STRING,
        createdTs: STRING,
        before: STRUCT<
            orderId: DECIMAL(20,0),
            orderRef: STRING,
            version: DECIMAL(10,0),
            orderDate: STRING,
            orderTs: STRING,
            orderStatus: STRING,
            orderType: STRING,
            totalAmount: DECIMAL(20,4),
            currency: STRING,
            customerId: STRING,
            shippingAddressId: STRING,
            createdTs: STRING
        >
    >> COMMENT 'Array of order records',
    orderDetails ARRAY<STRUCT<
        orderId: DECIMAL(20,0),
        version: DECIMAL(10,0),
        shippingMethod: STRING,
        trackingNumber: STRING,
        shippedTs: STRING,
        estimatedDeliveryDate: STRING,
        carrier: STRING,
        deliveryStatus: STRING
    >> COMMENT 'Array of order details',
    lineItems ARRAY<STRUCT<
        lineItemId: DECIMAL(20,0),
        orderId: DECIMAL(20,0),
        version: DECIMAL(10,0),
        productId: STRING,
        itemQty: DECIMAL(18,4),
        itemPrice: DECIMAL(18,8),
        itemAmount: DECIMAL(20,4),
        itemCurrency: STRING
    >> COMMENT 'Array of order line items'
)
USING DELTA
LOCATION '/tmp/delta-tables/order_stream_v2';


-- ============================================================================
-- SCHEMA ENFORCEMENT AND EVOLUTION SETTINGS
-- ============================================================================

-- Enable schema enforcement (default: true)
ALTER TABLE order_stream SET TBLPROPERTIES (
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2'
);

-- Allow schema evolution on writes
ALTER TABLE order_stream SET TBLPROPERTIES (
    'delta.autoMerge.enabled' = 'true'
);

-- Enable column mapping (required for column renames/drops in Delta 2.0+)
ALTER TABLE order_stream SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name'
);


-- ============================================================================
-- VIEW SCHEMA
-- ============================================================================

-- View current schema
SHOW CREATE TABLE order_stream;

-- View schema as JSON
SELECT schema FROM (DESCRIBE DETAIL order_stream);
