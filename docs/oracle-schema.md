# Orders Relational Data Model (With Versioning)

This model introduces a **version** column in all tables to support
**optimistic locking**, auditability, and safe concurrent updates.

------------------------------------------------------------------------

## 1. Parent Table: `orders`

Stores the core order record (one row per order).

``` sql
CREATE TABLE orders (
  order_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_ref           VARCHAR(64) NOT NULL UNIQUE,
  version             INTEGER NOT NULL DEFAULT 1,

  order_date          DATE NOT NULL,
  order_ts            TIMESTAMP NOT NULL,

  order_status        VARCHAR(16) NOT NULL CHECK (order_status IN ('PENDING','CONFIRMED','SHIPPED','DELIVERED','CANCELLED')),
  order_type          VARCHAR(16) NOT NULL CHECK (order_type IN ('STANDARD','EXPRESS','SUBSCRIPTION')),
  total_amount        NUMERIC(20,4) NOT NULL CHECK (total_amount > 0),
  currency            CHAR(3) NOT NULL,

  customer_id         VARCHAR(64) NOT NULL,
  shipping_address_id VARCHAR(64),

  created_ts          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

------------------------------------------------------------------------

## 2. Child Table (1:1): `order_details`

``` sql
CREATE TABLE order_details (
  order_id                BIGINT PRIMARY KEY,
  version                 INTEGER NOT NULL DEFAULT 1,

  shipping_method         VARCHAR(16) NOT NULL,
  tracking_number         VARCHAR(64) UNIQUE,
  shipped_ts              TIMESTAMP,
  estimated_delivery_date DATE,
  carrier                 VARCHAR(32),
  delivery_status         VARCHAR(16) CHECK (delivery_status IN ('PENDING','IN_TRANSIT','DELIVERED','RETURNED')),

  CONSTRAINT fk_order_details_order
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
    ON DELETE CASCADE
);
```

------------------------------------------------------------------------

## 3. Child Table (1:Many): `order_line_items`

``` sql
CREATE TABLE order_line_items (
  line_item_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_id      BIGINT NOT NULL,
  version       INTEGER NOT NULL DEFAULT 1,

  product_id    VARCHAR(64) NOT NULL,
  item_qty      NUMERIC(18,4) NOT NULL CHECK (item_qty > 0),
  item_price    NUMERIC(18,8),
  item_amount   NUMERIC(20,4),
  item_currency CHAR(3),

  CONSTRAINT fk_line_item_order
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
    ON DELETE CASCADE,

  CONSTRAINT uq_order_product UNIQUE (order_id, product_id)
);

CREATE INDEX ix_line_item_order_id ON order_line_items(order_id);
```

------------------------------------------------------------------------

## Relationships

-   **orders (1) → order_details (1)**
-   **orders (1) → order_line_items (N)**

------------------------------------------------------------------------

## Version Column Purpose

``` sql
UPDATE orders
SET order_status = 'CONFIRMED',
    version = version + 1
WHERE order_id = 1001
  AND version = 1;
```

------------------------------------------------------------------------

# Sample INSERT Statements

## Insert Parent Order

``` sql
INSERT INTO orders (
  order_ref,
  order_date,
  order_ts,
  order_status,
  order_type,
  total_amount,
  currency,
  customer_id,
  shipping_address_id
)
VALUES (
  'ORD-20260217-001',
  DATE '2026-02-17',
  TIMESTAMP '2026-02-17 10:15:30',
  'PENDING',
  'STANDARD',
  25250.0,
  'USD',
  'CUST-001',
  'ADDR-001'
);
```

------------------------------------------------------------------------

## Insert Order Details

``` sql
INSERT INTO order_details (
  order_id,
  shipping_method,
  tracking_number,
  carrier,
  delivery_status
)
VALUES (
  1,
  'EXPRESS',
  'TRK-00000001',
  'FEDEX',
  'PENDING'
);
```

------------------------------------------------------------------------

## Insert Line Items

``` sql
INSERT INTO order_line_items (
  order_id,
  product_id,
  item_qty,
  item_price,
  item_amount,
  item_currency
)
VALUES
  (1, 'PROD-100', 600, 25.50, 15300.00, 'USD'),
  (1, 'PROD-200', 400, 24.88, 9950.00, 'USD');
```

------------------------------------------------------------------------

# Bulk Test Data --- 100 Orders with 2--5 Line Items Each

``` sql
WITH ins_orders AS (
  INSERT INTO orders (
    order_ref,
    order_date,
    order_ts,
    order_status,
    order_type,
    total_amount,
    currency,
    customer_id,
    shipping_address_id
  )
  SELECT
    'ORD-20260217-' || LPAD(i::text, 3, '0'),
    DATE '2026-02-17',
    TIMESTAMP '2026-02-17 10:00:00' + (i * INTERVAL '5 seconds'),
    (ARRAY['PENDING','CONFIRMED','SHIPPED','DELIVERED','CANCELLED'])[1 + (i % 5)],
    (ARRAY['STANDARD','EXPRESS','SUBSCRIPTION'])[1 + (i % 3)],
    ((1000 + (i * 10)) * (25 + (i * 0.5)))::numeric(20,4),
    'USD',
    'CUST-' || LPAD(((i % 50) + 1)::text, 3, '0'),
    'ADDR-' || LPAD(((i % 30) + 1)::text, 3, '0')
  FROM generate_series(1, 100) AS gs(i)
  RETURNING order_id, total_amount, currency
),
item_plan AS (
  SELECT
    order_id,
    total_amount,
    currency,
    (2 + (order_id % 4))::int AS item_cnt
  FROM ins_orders
),
item_rows AS (
  SELECT
    p.order_id,
    p.currency,
    p.item_cnt,
    a.n AS item_n,
    (25 + (p.order_id * 0.5))::numeric(18,8) AS item_price,
    CASE
      WHEN a.n < p.item_cnt
        THEN TRUNC((1000 + (p.order_id * 10))::numeric / p.item_cnt, 4)
      ELSE
        (1000 + (p.order_id * 10))::numeric - (TRUNC((1000 + (p.order_id * 10))::numeric / p.item_cnt, 4) * (p.item_cnt - 1))
    END AS item_qty
  FROM item_plan p
  CROSS JOIN LATERAL generate_series(1, p.item_cnt) AS a(n)
)
INSERT INTO order_line_items (
  order_id,
  product_id,
  item_qty,
  item_price,
  item_amount,
  item_currency
)
SELECT
  order_id,
  'PROD-' || LPAD(((order_id * 10 + item_n) % 900 + 100)::text, 3, '0'),
  item_qty,
  item_price,
  item_qty * item_price,
  currency
FROM item_rows
ORDER BY order_id, item_n;
```

------------------------------------------------------------------------

# Design Notes

-   Versioning enables safe concurrent updates (optimistic locking)
-   Order details isolated for strict 1:1 mapping (shipping/fulfillment)
-   Line items normalized for scalability & partial order modifications
