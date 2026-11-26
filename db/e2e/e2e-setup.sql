/*
 * e2e-setup.sql: ground-truth log tables for the end-to-end suites (docs/E2E-TESTING.md).
 *
 * Run once, as APPUSER in ORCLPDB:
 *     sqlplus appuser/appuser@//localhost:1521/ORCLPDB @db/e2e/e2e-setup.sql
 *
 * Every committed test transaction inserts its rows here in the same transaction, so a
 * rolled-back transaction leaves nothing. ext04 captures only ORDERS, ORDER_DETAILS and
 * ORDER_LINE_ITEMS, so these tables never reach Kafka. ROWDEPENDENCIES makes ORA_ROWSCN the
 * exact commit SCN of each row, which is the CSN GoldenGate writes for that transaction.
 *
 * Re-running is safe: existing tables (and their history) are kept.
 */

SET SERVEROUTPUT ON
SET FEEDBACK OFF

DECLARE
  PROCEDURE create_if_missing(p_name VARCHAR2, p_ddl VARCHAR2) IS
    v_count NUMBER;
  BEGIN
    SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = p_name;
    IF v_count = 0 THEN
      EXECUTE IMMEDIATE p_ddl;
      DBMS_OUTPUT.PUT_LINE('Created ' || p_name);
    ELSE
      DBMS_OUTPUT.PUT_LINE(p_name || ' already exists, kept');
    END IF;
  END;
BEGIN
  -- One row per committed transaction
  create_if_missing('E2E_TX_LOG', q'[
    CREATE TABLE e2e_tx_log (
      run_id          VARCHAR2(32)  NOT NULL,
      session_no      NUMBER(5)     NOT NULL,
      tx_seq          NUMBER(10)    NOT NULL,
      kind            VARCHAR2(32)  NOT NULL,
      local_tx_id     VARCHAR2(64),
      commit_ts_utc   TIMESTAMP(6)  NOT NULL,
      orders_cnt      NUMBER(10)    NOT NULL,
      details_cnt     NUMBER(10)    NOT NULL,
      line_items_cnt  NUMBER(10)    NOT NULL,
      CONSTRAINT pk_e2e_tx_log PRIMARY KEY (run_id, session_no, tx_seq)
    ) ROWDEPENDENCIES]');

  -- One row per order a committed transaction touched
  create_if_missing('E2E_TX_ORDERS', q'[
    CREATE TABLE e2e_tx_orders (
      run_id      VARCHAR2(32) NOT NULL,
      session_no  NUMBER(5)    NOT NULL,
      tx_seq      NUMBER(10)   NOT NULL,
      order_id    NUMBER(19)   NOT NULL,
      CONSTRAINT pk_e2e_tx_orders PRIMARY KEY (run_id, session_no, tx_seq, order_id)
    ) ROWDEPENDENCIES]');
END;
/

SELECT table_name, dependencies FROM user_tables
 WHERE table_name IN ('E2E_TX_LOG', 'E2E_TX_ORDERS') ORDER BY table_name;

EXIT
