/*
 * e2e-cleanup.sql: removes end-to-end test data from the Oracle source (docs/E2E-TESTING.md).
 *
 * As APPUSER in ORCLPDB:
 *     sqlplus appuser/appuser@//localhost:1521/ORCLPDB @db/e2e/e2e-cleanup.sql <run-id>
 *         deletes the source rows and log rows of one run
 *     sqlplus appuser/appuser@//localhost:1521/ORCLPDB @db/e2e/e2e-cleanup.sql ALL
 *         the same for every run (orders whose customer_id starts with E2E-)
 *     sqlplus appuser/appuser@//localhost:1521/ORCLPDB @db/e2e/e2e-cleanup.sql --drop
 *         additionally drops the two log tables (undoes e2e-setup.sql)
 *
 * ext04 sets IGNOREDELETES, so these deletes never reach Kafka and the pipeline output in
 * Unity Catalog is left as it is (use scripts/reset-pipeline.sh to empty it).
 */

SET SERVEROUTPUT ON
SET FEEDBACK OFF
SET VERIFY OFF

DEFINE target = '&1'

DECLARE
  v_target VARCHAR2(64) := '&target';
  v_tag    VARCHAR2(64);
  v_runs   VARCHAR2(64);
  v_count  NUMBER;
BEGIN
  IF v_target IS NULL THEN
    RAISE_APPLICATION_ERROR(-20001, 'Usage: @db/e2e/e2e-cleanup.sql <run-id> | ALL | --drop');
  END IF;

  IF v_target IN ('ALL', '--drop') THEN
    v_tag  := 'E2E-%';
    v_runs := '%';
  ELSE
    v_tag  := 'E2E-' || v_target;
    v_runs := v_target;
  END IF;

  DELETE FROM order_line_items
   WHERE order_id IN (SELECT order_id FROM orders WHERE customer_id LIKE v_tag);
  v_count := SQL%ROWCOUNT;
  DELETE FROM order_details
   WHERE order_id IN (SELECT order_id FROM orders WHERE customer_id LIKE v_tag);
  DELETE FROM orders WHERE customer_id LIKE v_tag;
  DBMS_OUTPUT.PUT_LINE('Deleted ' || SQL%ROWCOUNT || ' orders and ' || v_count || ' line items');

  SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = 'E2E_TX_LOG';
  IF v_count > 0 THEN
    EXECUTE IMMEDIATE 'DELETE FROM e2e_tx_orders WHERE run_id LIKE :r' USING v_runs;
    EXECUTE IMMEDIATE 'DELETE FROM e2e_tx_log WHERE run_id LIKE :r' USING v_runs;
    DBMS_OUTPUT.PUT_LINE('Deleted ' || SQL%ROWCOUNT || ' log rows');
  END IF;
  COMMIT;

  IF v_target = '--drop' THEN
    FOR t IN (SELECT table_name FROM user_tables WHERE table_name IN ('E2E_TX_ORDERS', 'E2E_TX_LOG')) LOOP
      EXECUTE IMMEDIATE 'DROP TABLE ' || t.table_name || ' PURGE';
      DBMS_OUTPUT.PUT_LINE('Dropped ' || t.table_name);
    END LOOP;
  END IF;
END;
/

EXIT
