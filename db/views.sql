-- Convenience views over order_stream and orders_current.
--
-- Not run by SeedJob. The Unity Catalog Spark connector (UCSingleCatalog, before Spark 4.2) doesn't support
-- views ("Catalog mymanaged does not support views"), so these are session-scoped TEMPORARY
-- views. Run this file at the start of a spark-sql, spark-shell or Zeppelin session, then query
-- the views by their bare names (for example SELECT * FROM order_stream_changes).
--
-- Table names are fully qualified. Change mymanaged.argus1 if you use a different catalog or schema.

-- Create a view for easier querying (flattened from orders array)
CREATE OR REPLACE TEMPORARY VIEW order_stream_current AS
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
FROM mymanaged.argus1.order_stream;

-- Create a view showing changes (where before image exists in orders array)
CREATE OR REPLACE TEMPORARY VIEW order_stream_changes AS
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
FROM mymanaged.argus1.order_stream
LATERAL VIEW explode(orders) orders_table AS o
WHERE o.before IS NOT NULL;

-- Create a simple current view
CREATE OR REPLACE TEMPORARY VIEW orders_current_view AS
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
FROM mymanaged.argus1.orders_current;
