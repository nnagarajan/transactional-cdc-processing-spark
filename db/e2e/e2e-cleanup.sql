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
 * After the deletes, the three source tables and the log tables are shrunk (ALTER TABLE ...
 * SHRINK SPACE CASCADE), so the freed space is returned to the tablespace.
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

/*
 * Returns the space freed by the deletes: shrinks each table and its indexes (CASCADE).
 * SHRINK SPACE needs row movement and an ASSM tablespace; a table in a manual-segment-space
 * tablespace is skipped. Shrinking moves rows, so ORA_ROWSCN of the rows that remain changes:
 * the reports of runs that are kept can no longer be rebuilt from e2e_tx_log afterwards.
 */
DECLARE
  FUNCTION mb(p_table VARCHAR2) RETURN NUMBER IS
    v_mb NUMBER;
  BEGIN
    SELECT ROUND(NVL(SUM(bytes), 0) / 1048576, 1) INTO v_mb FROM user_segments
     WHERE segment_name = p_table
        OR segment_name IN (SELECT index_name FROM user_indexes WHERE table_name = p_table);
    RETURN v_mb;
  END;
BEGIN
  FOR t IN (SELECT ut.table_name, ts.segment_space_management AS ssm
              FROM user_tables ut JOIN user_tablespaces ts ON ts.tablespace_name = ut.tablespace_name
             WHERE ut.table_name IN ('ORDER_LINE_ITEMS', 'ORDER_DETAILS', 'ORDERS', 'E2E_TX_ORDERS', 'E2E_TX_LOG')
             ORDER BY ut.table_name) LOOP
    IF t.ssm <> 'AUTO' THEN
      DBMS_OUTPUT.PUT_LINE('Skipped shrinking ' || t.table_name || ': its tablespace does not use ASSM');
    ELSE
      DECLARE
        v_before NUMBER := mb(t.table_name);
      BEGIN
        EXECUTE IMMEDIATE 'ALTER TABLE ' || t.table_name || ' ENABLE ROW MOVEMENT';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || t.table_name || ' SHRINK SPACE CASCADE';
        DBMS_OUTPUT.PUT_LINE('Shrank ' || t.table_name || ' and its indexes: ' || v_before || ' MB -> ' || mb(t.table_name) || ' MB');
      END;
    END IF;
  END LOOP;
END;
/

EXIT
