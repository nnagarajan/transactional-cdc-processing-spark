-- Sample queries for the orders_current (SCD Type 1) Delta Lake table
-- orders_current has flattened order attributes, single orderDetails struct, lineItems array

-- Show table details
DESCRIBE DETAIL orders_current;

-- Show table history
DESCRIBE HISTORY orders_current LIMIT 10;

-- Count current orders (should be unique by orderId)
SELECT COUNT(*) as total_current_orders FROM orders_current;

-- Verify uniqueness of orderId
SELECT
    orderId,
    COUNT(*) as occurrence_count
FROM orders_current
GROUP BY orderId
HAVING COUNT(*) > 1;

-- Show latest 10 current orders
SELECT
    orderId,
    xid,
    csn,
    dwhProcessedTs,
    orderRef,
    orderStatus,
    totalAmount,
    currency,
    version as orderVersion,
    orderDetails.shippingMethod,
    orderDetails.carrier,
    size(lineItems) as line_item_count
FROM orders_current
ORDER BY dwhProcessedTs DESC
LIMIT 10;

-- Current orders by status
SELECT
    orderStatus,
    COUNT(*) as order_count,
    SUM(totalAmount) as total_amount,
    AVG(version) as avg_version
FROM orders_current
GROUP BY orderStatus
ORDER BY order_count DESC;

-- Current orders by currency
SELECT
    currency,
    COUNT(*) as count,
    AVG(version) as avg_version
FROM orders_current
GROUP BY currency;

-- Orders with highest versions (most updated)
SELECT
    orderId,
    orderRef,
    orderStatus,
    totalAmount,
    currency,
    version as orderVersion,
    dwhProcessedTs
FROM orders_current
ORDER BY version DESC
LIMIT 20;

-- Compare SCD Type 1 vs Type 2 record counts
SELECT
    'SCD Type 1 (Current)' as table_type,
    COUNT(*) as record_count
FROM orders_current
UNION ALL
SELECT
    'SCD Type 2 (Historical)' as table_type,
    COUNT(*) as record_count
FROM delta.`/tmp/delta-tables/order_stream`;

-- Current orders with details (single struct access)
SELECT
    orderId,
    orderRef,
    orderStatus,
    version as order_version,
    orderDetails.shippingMethod,
    orderDetails.trackingNumber,
    orderDetails.carrier,
    orderDetails.deliveryStatus,
    orderDetails.version as detail_version
FROM orders_current
LIMIT 20;

-- Current orders with line items breakdown
SELECT
    o.orderId,
    o.orderRef,
    li.lineItemId,
    li.productId,
    li.itemQty,
    li.itemPrice,
    li.version as line_item_version
FROM orders_current o
LATERAL VIEW explode(o.lineItems) line_items_table AS li
LIMIT 20;

-- Identify orders that have been updated (have before image)
SELECT
    orderId,
    orderRef,
    version as current_version,
    orderBefore.version as previous_version,
    totalAmount as current_amount,
    orderBefore.totalAmount as previous_amount
FROM orders_current
WHERE orderBefore IS NOT NULL
LIMIT 20;

-- Recent updates (by dwhProcessedTs)
SELECT
    orderId,
    orderRef,
    orderStatus,
    totalAmount,
    currency,
    version,
    dwhProcessedTs
FROM orders_current
ORDER BY dwhProcessedTs DESC
LIMIT 20;

-- Orders with version mismatches between entities
SELECT
    orderId,
    version as order_version,
    orderDetails.version as detail_version,
    aggregate(lineItems, CAST(0.0 AS DOUBLE), (acc, li) -> GREATEST(acc, li.version)) as max_line_item_version
FROM orders_current
WHERE
    orderDetails.version != version OR
    aggregate(lineItems, CAST(0.0 AS DOUBLE), (acc, li) -> GREATEST(acc, li.version)) != version
LIMIT 20;

-- Order detail changes (compare before/after)
SELECT
    orderId,
    orderRef,
    orderDetails.shippingMethod as current_shipping_method,
    orderDetails.before.shippingMethod as previous_shipping_method,
    orderDetails.deliveryStatus as current_delivery_status,
    orderDetails.before.deliveryStatus as previous_delivery_status
FROM orders_current
WHERE orderDetails.before IS NOT NULL
LIMIT 20;

-- Change Data Feed (if enabled)
-- SELECT * FROM table_changes('orders_current', 2, 5);
