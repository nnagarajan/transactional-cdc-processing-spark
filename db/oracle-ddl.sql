/* Oracle 12c+ compatible DDL + bulk inserts (100 orders, 2–5 line items each) */

--------------------------------------------------------------------------------
-- DDL
--------------------------------------------------------------------------------

CREATE TABLE orders (
  order_id            NUMBER(19,0) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_ref           VARCHAR2(64) NOT NULL,
  version             NUMBER(10,0) DEFAULT 1 NOT NULL,
  order_date          DATE NOT NULL,
  order_ts            TIMESTAMP NOT NULL,
  order_status        VARCHAR2(16) NOT NULL,
  order_type          VARCHAR2(16) NOT NULL,
  total_amount        NUMBER(20,4) NOT NULL,
  currency            CHAR(3) NOT NULL,
  customer_id         VARCHAR2(64) NOT NULL,
  shipping_address_id VARCHAR2(64),
  created_ts          TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT uq_order_ref UNIQUE (order_ref),
  CONSTRAINT ck_order_status CHECK (order_status IN ('PENDING','CONFIRMED','SHIPPED','DELIVERED','CANCELLED')),
  CONSTRAINT ck_order_type CHECK (order_type IN ('STANDARD','EXPRESS','SUBSCRIPTION')),
  CONSTRAINT ck_order_amount CHECK (total_amount > 0)
);

CREATE TABLE order_details (
  order_id                NUMBER(19,0) PRIMARY KEY,
  version                 NUMBER(10,0) DEFAULT 1 NOT NULL,
  shipping_method         VARCHAR2(16) NOT NULL,
  tracking_number         VARCHAR2(64),
  shipped_ts              TIMESTAMP,
  estimated_delivery_date DATE,
  carrier                 VARCHAR2(32),
  delivery_status         VARCHAR2(16),
  CONSTRAINT uq_order_details_tracking UNIQUE (tracking_number),
  CONSTRAINT ck_delivery_status CHECK (delivery_status IN ('PENDING','IN_TRANSIT','DELIVERED','RETURNED')),
  CONSTRAINT fk_order_details_order FOREIGN KEY (order_id)
    REFERENCES orders(order_id)
);

CREATE TABLE order_line_items (
  line_item_id  NUMBER(19,0) GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_id      NUMBER(19,0) NOT NULL,
  version       NUMBER(10,0) DEFAULT 1 NOT NULL,
  product_id    VARCHAR2(64) NOT NULL,
  item_qty      NUMBER(18,4) NOT NULL,
  item_price    NUMBER(18,8),
  item_amount   NUMBER(20,4),
  item_currency CHAR(3),
  CONSTRAINT fk_line_item_order FOREIGN KEY (order_id)
    REFERENCES orders(order_id),
  CONSTRAINT uq_order_product UNIQUE (order_id, product_id),
  CONSTRAINT ck_item_qty CHECK (item_qty > 0)
);

CREATE INDEX ix_line_item_order_id ON order_line_items(order_id);

--------------------------------------------------------------------------------
-- Version Triggers: auto-increment VERSION on update
--------------------------------------------------------------------------------

CREATE OR REPLACE TRIGGER trg_orders_version
  BEFORE UPDATE ON orders
  FOR EACH ROW
BEGIN
  :NEW.version := :OLD.version + 1;
END;
/

CREATE OR REPLACE TRIGGER trg_order_details_version
  BEFORE UPDATE ON order_details
  FOR EACH ROW
BEGIN
  :NEW.version := :OLD.version + 1;
END;
/

CREATE OR REPLACE TRIGGER trg_order_line_items_version
  BEFORE UPDATE ON order_line_items
  FOR EACH ROW
BEGIN
  :NEW.version := :OLD.version + 1;
END;
/

--------------------------------------------------------------------------------
-- Bulk Inserts: 100 orders, each order has 2–5 line items
-- (item_cnt = 2 + MOD(order_id, 4)) and quantities split to sum exactly.
--------------------------------------------------------------------------------

DECLARE
  v_order_id    NUMBER;
  v_qty         NUMBER(18,4);
  v_price       NUMBER(18,8);
  v_currency    CHAR(3) := 'USD';
  v_item_cnt    PLS_INTEGER;
  v_base_qty    NUMBER(18,4);
  v_rem_qty     NUMBER(18,4);
  v_total       NUMBER(20,4);
  v_statuses    SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('PENDING','CONFIRMED','SHIPPED','DELIVERED','CANCELLED');
  v_types       SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('STANDARD','EXPRESS','SUBSCRIPTION');
  v_methods     SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('STANDARD','EXPRESS','OVERNIGHT','PICKUP');
  v_carriers    SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('FEDEX','UPS','DHL','USPS');
  v_del_status  SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('PENDING','IN_TRANSIT','DELIVERED','RETURNED');
BEGIN
  FOR i IN 1..100 LOOP
    v_qty   := 1000 + (i * 10);
    v_price := 25 + (i * 0.5);
    v_total := v_qty * v_price;

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
      'ORD-20260217-' || LPAD(i, 3, '0'),
      DATE '2026-02-17',
      TIMESTAMP '2026-02-17 10:00:00' + NUMTODSINTERVAL(i * 5, 'SECOND'),
      v_statuses(MOD(i, 5) + 1),
      v_types(MOD(i, 3) + 1),
      v_total,
      v_currency,
      'CUST-' || LPAD(MOD(i, 50) + 1, 3, '0'),
      'ADDR-' || LPAD(MOD(i, 30) + 1, 3, '0')
    )
    RETURNING order_id INTO v_order_id;

    -- Shipping / fulfillment details (1:1)
    INSERT INTO order_details (
      order_id,
      shipping_method,
      tracking_number,
      carrier,
      delivery_status
    )
    VALUES (
      v_order_id,
      v_methods(MOD(i, 4) + 1),
      'TRK-' || LPAD(v_order_id, 8, '0'),
      v_carriers(MOD(i, 4) + 1),
      v_del_status(MOD(i, 4) + 1)
    );

    -- 2..5 line items per order
    v_item_cnt := 2 + MOD(v_order_id, 4);

    v_base_qty := TRUNC(v_qty / v_item_cnt, 4);
    v_rem_qty  := v_qty - (v_base_qty * (v_item_cnt - 1));

    FOR n IN 1..v_item_cnt LOOP
      INSERT INTO order_line_items (
        order_id,
        product_id,
        item_qty,
        item_price,
        item_amount,
        item_currency
      )
      VALUES (
        v_order_id,
        'PROD-' || LPAD(MOD((v_order_id * 10 + n), 900) + 100, 3, '0'),
        CASE WHEN n < v_item_cnt THEN v_base_qty ELSE v_rem_qty END,
        v_price,
        CASE WHEN n < v_item_cnt THEN v_base_qty ELSE v_rem_qty END * v_price,
        v_currency
      );
    END LOOP;

  END LOOP;

  COMMIT;
END;
/



ALTER TRIGGER trg_orders_version COMPILE;
ALTER TRIGGER trg_order_details_version COMPILE;
ALTER TRIGGER trg_order_line_items_version COMPILE;