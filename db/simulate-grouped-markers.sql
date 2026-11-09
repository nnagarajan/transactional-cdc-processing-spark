/*
 * simulate-grouped-markers.sql
 *
 * Reproduces the grouped-delivery marker bug: with GROUPTRANSOPS 1000 in rep3.prm,
 * Replicat packs several source transactions (different xid:csn) into one delivery,
 * and the original TransactionBoundaryEventJsonFormatter writes ONE marker for the
 * whole delivery:
 *   - xid and csn of the FIRST source transaction only
 *   - per-table counts summed over ALL source transactions in the delivery
 *
 * The pipeline then expects 10 events for transaction 1, which only has 4, so
 * transaction 1 never completes and blocks every later transaction forever.
 * Transactions 2 and 3 never get a marker at all.
 *
 * The script commits three small transactions with distinct, recognisable counts:
 *
 *   tx  ORDERS  ORDER_DETAILS  ORDER_LINE_ITEMS  total
 *   1      1          1               2            4
 *   2      1          1               3            5
 *   3      1          0               0            1
 *                                         sum:    10
 *
 * Prerequisites: db/oracle-ddl.sql has been run (seed data present), as APPUSER in ORCLPDB.
 *
 * Steps
 * -----
 * 1. Make Replicat read the three transactions as a backlog, so it groups them:
 *        ggsci> STOP REPLICAT rep3
 *    (Leave ext04 and kc running. kc publishes the data events as usual.)
 *
 * 2. Run this script:
 *        sqlplus appuser/<password>@ORCLPDB @db/simulate-grouped-markers.sql
 *    It prints the xid and commit SCN (= GoldenGate CSN) of each transaction.
 *
 * 3. Start Replicat. It reads all 10 operations at once; 10 < GROUPTRANSOPS 1000,
 *    so they go out as a single delivery:
 *        ggsci> START REPLICAT rep3
 *
 * 4. Read the newest marker:
 *        kafka-console-consumer --bootstrap-server localhost:9092 \
 *          --topic dev.transaction_metadata_json --from-beginning --timeout-ms 10000
 *
 *    Original formatter (the bug): one object, tx 1's csn, summed counts
 *        {"xid":"<tx1>","csn":"<csn1>","event_count":10,
 *         "data_collections":[{"data_collection":"ORDERS","event_count":3},
 *                             {"data_collection":"ORDER_DETAILS","event_count":2},
 *                             {"data_collection":"ORDER_LINE_ITEMS","event_count":5}]}
 *
 *    Fixed formatter (gg-formatter/): one message holding an array of three markers
 *        [{"csn":"<csn1>","event_count":4,...},
 *         {"csn":"<csn2>","event_count":5,...},
 *         {"csn":"<csn3>","event_count":1,...}]
 *
 *    Compare the marker with the "expected markers" report printed at the end of this script.
 *
 * 5. In the pipeline (TransactionalCdcProcessingApp) with the original formatter:
 *    no row for any of the three orders appears in order_stream, and after
 *    job.ordering.blocked-alert-threshold the log reports csn1 as a blocked head.
 *    See STATE-STORE-DEBUG.md to inspect the stuck buffer (expected 10, received 4).
 *
 * Note: the same-CSN case (two transactions committing at one SCN) can't be forced
 * from a single session; this script covers different CSNs only.
 */

SET SERVEROUTPUT ON
SET FEEDBACK OFF
SET LINESIZE 200

--------------------------------------------------------------------------------
-- Log table, NOT captured by ext04 (it lists only the three order tables).
-- ROWDEPENDENCIES makes ORA_ROWSCN the exact commit SCN of each row, which is
-- the CSN GoldenGate writes for that transaction.
--------------------------------------------------------------------------------
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE sim_grouped_tx_log PURGE';
EXCEPTION WHEN OTHERS THEN
  IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE sim_grouped_tx_log (
  run_id           VARCHAR2(32),
  tx_no            NUMBER,
  local_tx_id      VARCHAR2(64),
  order_id         NUMBER,
  orders_cnt       NUMBER,
  details_cnt      NUMBER,
  line_items_cnt   NUMBER
) ROWDEPENDENCIES;

--------------------------------------------------------------------------------
-- Three transactions, each committed separately.
--------------------------------------------------------------------------------
DECLARE
  v_run_id  VARCHAR2(32) := TO_CHAR(SYSTIMESTAMP, 'YYYYMMDD-HH24MISS');
  v_a NUMBER;  -- order for tx 1 (needs >= 2 line items)
  v_b NUMBER;  -- order for tx 2 (needs >= 3 line items)
  v_c NUMBER;  -- order for tx 3

  PROCEDURE run_tx(p_tx_no NUMBER, p_order_id NUMBER, p_details NUMBER, p_items NUMBER) IS
    v_items NUMBER;
  BEGIN
    UPDATE orders
       SET order_status = CASE order_status WHEN 'PENDING' THEN 'CONFIRMED' ELSE 'PENDING' END
     WHERE order_id = p_order_id;

    IF p_details > 0 THEN
      UPDATE order_details
         SET carrier = CASE carrier WHEN 'UPS' THEN 'FEDEX' ELSE 'UPS' END
       WHERE order_id = p_order_id;
    END IF;

    IF p_items > 0 THEN
      UPDATE order_line_items
         SET item_price = item_price + 0.01,
             item_amount = item_qty * (item_price + 0.01)
       WHERE line_item_id IN (SELECT line_item_id FROM order_line_items
                               WHERE order_id = p_order_id
                               ORDER BY line_item_id
                               FETCH FIRST p_items ROWS ONLY);
      v_items := SQL%ROWCOUNT;
      IF v_items != p_items THEN
        RAISE_APPLICATION_ERROR(-20001, 'order ' || p_order_id || ' has only ' || v_items || ' line items');
      END IF;
    END IF;

    INSERT INTO sim_grouped_tx_log
    VALUES (v_run_id, p_tx_no, DBMS_TRANSACTION.LOCAL_TRANSACTION_ID, p_order_id, 1, p_details, p_items);

    COMMIT;
  END;
BEGIN
  SELECT MIN(order_id) INTO v_a
    FROM (SELECT order_id FROM order_line_items GROUP BY order_id HAVING COUNT(*) >= 2);

  SELECT MIN(order_id) INTO v_b
    FROM (SELECT order_id FROM order_line_items GROUP BY order_id HAVING COUNT(*) >= 3)
   WHERE order_id != v_a;

  SELECT MIN(order_id) INTO v_c
    FROM orders WHERE order_id NOT IN (v_a, v_b);

  run_tx(1, v_a, 1, 2);
  run_tx(2, v_b, 1, 3);
  run_tx(3, v_c, 0, 0);

  DBMS_OUTPUT.PUT_LINE('Committed 3 transactions, run ' || v_run_id);
END;
/

--------------------------------------------------------------------------------
-- Expected markers: what a correct formatter publishes, one per transaction.
--------------------------------------------------------------------------------
PROMPT
PROMPT Expected markers (one per source transaction):
COLUMN csn FORMAT 99999999999999
SELECT tx_no,
       local_tx_id,
       ORA_ROWSCN AS csn,
       order_id,
       orders_cnt      AS "ORDERS",
       details_cnt     AS "ORDER_DETAILS",
       line_items_cnt  AS "ORDER_LINE_ITEMS",
       orders_cnt + details_cnt + line_items_cnt AS event_count
  FROM sim_grouped_tx_log
 ORDER BY tx_no;

PROMPT
PROMPT What the ORIGINAL formatter publishes for the grouped delivery (the bug):
SELECT MIN(ORA_ROWSCN) KEEP (DENSE_RANK FIRST ORDER BY tx_no) AS csn,
       SUM(orders_cnt)     AS "ORDERS",
       SUM(details_cnt)    AS "ORDER_DETAILS",
       SUM(line_items_cnt) AS "ORDER_LINE_ITEMS",
       SUM(orders_cnt + details_cnt + line_items_cnt) AS event_count
  FROM sim_grouped_tx_log;

PROMPT
PROMPT Now run: ggsci> START REPLICAT rep3
PROMPT Then compare dev.transaction_metadata_json with the reports above.
