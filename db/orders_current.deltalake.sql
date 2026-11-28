-- Written by `scripts/cdc-model.sh ddl` from the orders job config. Do not edit by hand: change the
-- job config and write it again.
-- Stage 2 table of job orders: the current state per root key.

DROP TABLE IF EXISTS {{CATALOG_SCHEMA}}.orders_current;

CREATE TABLE IF NOT EXISTS {{CATALOG_SCHEMA}}.orders_current (
    xid STRING,
    csn STRING,
    dwhProcessedTs STRING,
    orderId DOUBLE,
    orderRef STRING,
    version DOUBLE,
    orderDate STRING,
    orderTs STRING,
    orderStatus STRING,
    orderType STRING,
    totalAmount DOUBLE,
    currency STRING,
    customerId STRING,
    shippingAddressId STRING,
    createdTs STRING,
    orderBefore STRUCT<orderId: DOUBLE, orderRef: STRING, version: DOUBLE, orderDate: STRING, orderTs: STRING, orderStatus: STRING, orderType: STRING, totalAmount: DOUBLE, currency: STRING, customerId: STRING, shippingAddressId: STRING, createdTs: STRING>,
    orderDetails STRUCT<orderId: DOUBLE, version: DOUBLE, shippingMethod: STRING, trackingNumber: STRING, shippedTs: STRING, estimatedDeliveryDate: STRING, carrier: STRING, deliveryStatus: STRING, before: STRUCT<orderId: DOUBLE, version: DOUBLE, shippingMethod: STRING, trackingNumber: STRING, shippedTs: STRING, estimatedDeliveryDate: STRING, carrier: STRING, deliveryStatus: STRING>>,
    lineItems ARRAY<STRUCT<lineItemId: DOUBLE, orderId: DOUBLE, version: DOUBLE, productId: STRING, itemQty: DOUBLE, itemPrice: DOUBLE, itemAmount: DOUBLE, itemCurrency: STRING, before: STRUCT<lineItemId: DOUBLE, orderId: DOUBLE, version: DOUBLE, productId: STRING, itemQty: DOUBLE, itemPrice: DOUBLE, itemAmount: DOUBLE, itemCurrency: STRING>>>
)
USING DELTA
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');
