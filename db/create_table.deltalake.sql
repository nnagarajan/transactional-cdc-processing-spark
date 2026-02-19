-- Create Delta Lake table for order_stream (SCD Type 2)
-- This table stores denormalized order records with nested order attributes,
-- order details, and line items — all as arrays of structs with before images.

-- Drop table if exists (for recreation)
DROP TABLE IF EXISTS default.order_stream;

-- Create external Delta Lake table with nested schema
CREATE TABLE IF NOT EXISTS default.order_stream (
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
USING DELTA;

-- Create a view for easier querying (flattened from orders array)
CREATE OR REPLACE VIEW default.order_stream_current AS
SELECT
    orderId,
    xid,
    csn,
    dwhProcessedTs,
    orders[0].orderRef as orderRef,
    orders[0].orderStatus as orderStatus,
    orders[0].totalAmount as totalAmount,
    orders[0].currency as currency,
    orders[0].orderDate as orderDate,
    orders[0].version as orderVersion,
    size(orderDetails) as detail_count,
    size(lineItems) as line_item_count
FROM order_stream;

-- Create a view showing changes (where before image exists in orders array)
CREATE OR REPLACE VIEW default.order_stream_changes AS
SELECT
    orderId,
    xid,
    csn,
    dwhProcessedTs,
    o.orderRef,
    o.orderStatus,
    o.totalAmount,
    o.version as orderVersion,
    o.before.totalAmount as previous_total_amount,
    o.before.orderStatus as previous_status,
    o.before.version as previous_version,
    CASE
        WHEN o.before IS NOT NULL THEN 'UPDATED'
        ELSE 'INSERTED'
    END as change_type
FROM order_stream
LATERAL VIEW explode(orders) orders_table AS o
WHERE o.before IS NOT NULL;

-- Show table description
DESCRIBE EXTENDED default.order_stream;
