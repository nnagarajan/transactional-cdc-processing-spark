-- Create SCD Type 1 (current state only) table for orders
-- This table maintains only the latest version of each order with flattened attributes.
-- orderDetails is a single struct (1:1), lineItems is an array (1:N).

-- Drop table if exists (for recreation)
DROP TABLE IF EXISTS default.orders_current;

-- Create SCD Type 1 table with flattened order attributes
CREATE TABLE IF NOT EXISTS default.orders_current (
    -- Transaction identifiers (from last update)
    xid STRING COMMENT 'Oracle transaction ID from last update',
    csn STRING COMMENT 'Oracle commit sequence number from last update',

    -- Metadata
    dwhProcessedTs STRING COMMENT 'Timestamp of last update to this record',

    -- Flattened order attributes (current version only)
    orderId DOUBLE COMMENT 'Primary order identifier (unique key)',
    orderRef STRING COMMENT 'Current order reference',
    version DOUBLE COMMENT 'Current order version number',
    orderDate STRING COMMENT 'Current order date',
    orderTs STRING COMMENT 'Current order timestamp',
    orderStatus STRING COMMENT 'Current order status',
    orderType STRING COMMENT 'Current order type',
    totalAmount DOUBLE COMMENT 'Current total order amount',
    currency STRING COMMENT 'Current order currency',
    customerId STRING COMMENT 'Current customer identifier',
    shippingAddressId STRING COMMENT 'Current shipping address identifier',
    createdTs STRING COMMENT 'Record creation timestamp',

    -- Before image preserved from SCD Type 2 (for audit)
    orderBefore STRUCT<
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
    > COMMENT 'Before image from last change',

    -- Order details (current version only, single struct — 1:1 with order)
    orderDetails STRUCT<
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
    > COMMENT 'Current order details (latest version)',

    -- Order line items array (current versions only — 1:N with order)
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
    >> COMMENT 'Array of current order line items (each at its latest version)'
)
USING DELTA;

-- Create a simple current view
CREATE OR REPLACE VIEW default.orders_current_view AS
SELECT
    orderId,
    orderRef,
    orderStatus,
    totalAmount,
    currency,
    orderDate,
    version as orderVersion,
    dwhProcessedTs,
    orderDetails.shippingMethod,
    orderDetails.carrier,
    orderDetails.deliveryStatus,
    size(lineItems) as line_item_count
FROM orders_current;

-- Show table description
DESCRIBE EXTENDED default.orders_current;
