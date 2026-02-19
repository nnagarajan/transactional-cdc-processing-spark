-- Sample queries for the order_stream Delta Lake table (SCD Type 2)
-- order_stream has nested orders array — use explode or [0] to access order attributes

-- Show table version history
DESCRIBE HISTORY order_stream LIMIT 10;

-- Show table details
DESCRIBE DETAIL order_stream;

-- Count total order records
SELECT COUNT(*) as total_records FROM order_stream;

-- Show latest 10 orders (extract from orders array)
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
FROM order_stream
ORDER BY orderId DESC
LIMIT 10;

-- Orders by status (explode orders array)
SELECT
    o.orderStatus,
    COUNT(*) as order_count,
    SUM(o.totalAmount) as total_amount,
    AVG(o.totalAmount) as avg_amount
FROM order_stream
LATERAL VIEW explode(orders) orders_table AS o
GROUP BY o.orderStatus
ORDER BY order_count DESC;

-- Orders by currency
SELECT
    o.currency,
    COUNT(*) as order_count,
    SUM(o.totalAmount) as total_amount
FROM order_stream
LATERAL VIEW explode(orders) orders_table AS o
GROUP BY o.currency
ORDER BY order_count DESC;

-- Orders with their details (exploded view)
SELECT
    t.orderId,
    o.orderRef,
    o.orderStatus,
    d.shippingMethod,
    d.trackingNumber,
    d.carrier,
    d.deliveryStatus,
    d.version as detail_version
FROM order_stream t
LATERAL VIEW explode(t.orders) orders_table AS o
LATERAL VIEW explode(t.orderDetails) details_table AS d
LIMIT 20;

-- Orders with their line items (exploded view)
SELECT
    t.orderId,
    orders[0].orderRef as orderRef,
    orders[0].orderStatus as orderStatus,
    li.lineItemId,
    li.productId,
    li.itemQty,
    li.itemPrice,
    li.itemAmount,
    li.itemCurrency,
    li.version as line_item_version
FROM order_stream t
LATERAL VIEW explode(t.lineItems) line_items_table AS li
LIMIT 20;

-- Orders by transaction (xid)
SELECT
    xid,
    csn,
    COUNT(*) as orders_in_transaction,
    SUM(orders[0].totalAmount) as total_amount
FROM order_stream
GROUP BY xid, csn
ORDER BY orders_in_transaction DESC
LIMIT 10;

-- Orders by customer
SELECT
    o.customerId,
    COUNT(*) as order_count,
    SUM(o.totalAmount) as total_amount
FROM order_stream
LATERAL VIEW explode(orders) orders_table AS o
GROUP BY o.customerId
ORDER BY order_count DESC
LIMIT 20;

-- Orders with line items breakdown
SELECT
    orderId,
    orders[0].orderRef as orderRef,
    orders[0].orderStatus as orderStatus,
    orders[0].totalAmount as totalAmount,
    orders[0].currency as currency,
    size(orderDetails) as num_details,
    size(lineItems) as num_line_items,
    aggregate(lineItems, CAST(0.0 AS DOUBLE), (acc, li) -> acc + li.itemQty) as total_item_qty
FROM order_stream
ORDER BY total_item_qty DESC
LIMIT 20;

-- Show orders with before images (updated records)
SELECT
    orderId,
    xid,
    dwhProcessedTs,
    o.totalAmount as current_amount,
    o.before.totalAmount as previous_amount,
    o.orderStatus as current_status,
    o.before.orderStatus as previous_status,
    o.version as current_version,
    o.before.version as previous_version
FROM order_stream
LATERAL VIEW explode(orders) orders_table AS o
WHERE o.before IS NOT NULL
LIMIT 20;

-- Show order detail changes (compare before/after)
SELECT
    t.orderId,
    d.shippingMethod as current_shipping_method,
    d.before.shippingMethod as previous_shipping_method,
    d.deliveryStatus as current_delivery_status,
    d.before.deliveryStatus as previous_delivery_status,
    d.version as current_version,
    d.before.version as previous_version
FROM order_stream t
LATERAL VIEW explode(t.orderDetails) details_table AS d
WHERE d.before IS NOT NULL
LIMIT 20;

-- Show line item changes (compare before/after)
SELECT
    t.orderId,
    li.lineItemId,
    li.productId,
    li.itemQty as current_qty,
    li.before.itemQty as previous_qty,
    li.itemQty - li.before.itemQty as qty_change,
    li.version as current_version,
    li.before.version as previous_version
FROM order_stream t
LATERAL VIEW explode(t.lineItems) line_items_table AS li
WHERE li.before IS NOT NULL
LIMIT 20;

-- Orders grouped by version
SELECT
    o.version as orderVersion,
    COUNT(*) as order_count
FROM order_stream
LATERAL VIEW explode(orders) orders_table AS o
GROUP BY o.version
ORDER BY o.version;

-- Time travel query (view data as of version 0)
SELECT COUNT(*) as count_at_version_0
FROM order_stream VERSION AS OF 0;

-- Time travel query (view data as of specific timestamp)
-- SELECT * FROM order_stream TIMESTAMP AS OF '2026-02-16 10:00:00';
