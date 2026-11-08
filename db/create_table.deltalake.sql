-- Create Delta Lake table for order_stream (SCD Type 2)
-- This table stores denormalized order records with nested order attributes,
-- order details, and line items — all as arrays of structs with before images.

-- Drop table if exists (for recreation)
DROP TABLE IF EXISTS {{CATALOG_SCHEMA}}.order_stream;

-- Create a Unity Catalog managed Delta table (UC picks the storage path under the schema's storage root)
CREATE TABLE IF NOT EXISTS {{CATALOG_SCHEMA}}.order_stream (
    -- Transaction identifiers
    xid STRING COMMENT 'Oracle transaction ID (xid)',
    csn STRING COMMENT 'Oracle commit sequence number',
    dwhProcessedTs STRING COMMENT 'Timestamp when record was processed by the streaming application',
    orderId DOUBLE COMMENT 'Primary order identifier',

    -- Orders array with before images
    orders ARRAY<STRUCT<
        orderId: DOUBLE,
        orderRef: STRING,
        version: DOUBLE,
        orderDate: STRING,
        orderTs: STRING,
        orderStatus: STRING,
        orderType: STRING,
        totalAmount: DOUBLE,
        currency: STRING,
        customerId: STRING,
        shippingAddressId: STRING,
        createdTs: STRING,
        before: STRUCT<
            orderId: DOUBLE,
            orderRef: STRING,
            version: DOUBLE,
            orderDate: STRING,
            orderTs: STRING,
            orderStatus: STRING,
            orderType: STRING,
            totalAmount: DOUBLE,
            currency: STRING,
            customerId: STRING,
            shippingAddressId: STRING,
            createdTs: STRING
        >
    >> COMMENT 'Array of order records with before images',

    -- Order details array with before images
    orderDetails ARRAY<STRUCT<
        orderId: DOUBLE,
        version: DOUBLE,
        shippingMethod: STRING,
        trackingNumber: STRING,
        shippedTs: STRING,
        estimatedDeliveryDate: STRING,
        carrier: STRING,
        deliveryStatus: STRING,
        before: STRUCT<
            orderId: DOUBLE,
            version: DOUBLE,
            shippingMethod: STRING,
            trackingNumber: STRING,
            shippedTs: STRING,
            estimatedDeliveryDate: STRING,
            carrier: STRING,
            deliveryStatus: STRING
        >
    >> COMMENT 'Array of order details with before images',

    -- Order line items array with before images
    lineItems ARRAY<STRUCT<
        lineItemId: DOUBLE,
        orderId: DOUBLE,
        version: DOUBLE,
        productId: STRING,
        itemQty: DOUBLE,
        itemPrice: DOUBLE,
        itemAmount: DOUBLE,
        itemCurrency: STRING,
        before: STRUCT<
            lineItemId: DOUBLE,
            orderId: DOUBLE,
            version: DOUBLE,
            productId: STRING,
            itemQty: DOUBLE,
            itemPrice: DOUBLE,
            itemAmount: DOUBLE,
            itemCurrency: STRING
        >
    >> COMMENT 'Array of order line items with before images'
)
USING DELTA
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');

-- Views are in db/views.sql: the Unity Catalog Spark connector (0.4.0) doesn't support views.

-- Show table description
DESCRIBE EXTENDED {{CATALOG_SCHEMA}}.order_stream;
