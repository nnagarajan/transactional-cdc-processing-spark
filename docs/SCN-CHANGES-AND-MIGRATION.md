# SCN Changes, Upgrades and Migrations

How the Oracle SCN behaves across database events such as recovery, upgrades and migrations, what each one does to this pipeline, and how to cut over safely.

## Why it matters

GoldenGate writes each transaction's commit SCN as its **CSN**. The sequencer's `Cursor` stores the last emitted CSN and compares CSNs **numerically**. Any marker whose CSN is below the cursor is treated as already emitted and **dropped, with only a WARN logged** (see Known Limitations in `CLAUDE.md`).

So the pipeline relies on CSNs only ever increasing. Anything that makes new commits arrive with lower SCNs causes silent data loss.

## How the SCN behaves

In normal running the SCN only ever increases, and a committed transaction's SCN never changes.

| Situation | What happens to the SCN | Effect on the pipeline |
|---|---|---|
| Normal operation | Increases with every commit; RAC instances share one SCN sequence | Safe |
| Distributed queries and database links | Can **jump forward** sharply, since both databases sync to the higher of their two SCNs | Safe: order is preserved |
| Flashback Database, or point-in-time recovery followed by `OPEN RESETLOGS` | Goes back to an older SCN and starts a new incarnation. New SCNs are **lower than ones already used**, and numerically can **repeat** values from the abandoned timeline | New transactions dropped until the SCN passes the old high-water mark |
| Moving the source to an unrelated database | That database has its own SCN sequence | Unpredictable: may be dropped |
| Data Guard switchover or failover to a physical standby | Same SCN sequence continues | Safe, but after a failover, commits the standby never received are lost |

## Version upgrades

An **in-place upgrade** (AutoUpgrade, DBUA, or a manual upgrade such as 19c to 23ai) keeps the database. The datafiles, the redo and the SCN all continue. **The CSN sequence is safe.**

The upgrade writes dictionary changes but doesn't touch `ORDERS`, `ORDER_DETAILS` or `ORDER_LINE_ITEMS`, so nothing extra reaches the topics. The work is on the GoldenGate side:

- Stop `ext04` before the upgrade.
- Integrated Extract usually has to be registered again afterwards.
- Check that your GoldenGate version supports the new database version.
- If GoldenGate is upgraded too, rebuild the custom marker formatter (`gg-formatter/`) against the new GoldenGate jars.

## Migrating on-prem to cloud

| Method | Same SCN sequence? | Effect on the pipeline |
|---|---|---|
| **Data Guard standby in the cloud, then switchover** (ZDM physical online, or manual) | Yes | SCNs keep increasing. This is the safest option |
| **RMAN restore or duplicate** to a new host | Starts from the SCN of the backup or restore point, then runs as its own timeline | If the source kept taking commits after the backup, new SCNs can be **lower** than CSNs already emitted |
| **Transportable tablespaces, or PDB unplug/plug/relocate** into a different CDB | No. The SCN belongs to the target CDB | Could be lower or higher. Don't rely on it either way |
| **Logical: Data Pump, GoldenGate replication, ZDM logical** | No. It's an unrelated database | SCNs are unrelated to the source's |
| **Managed services (RDS, Autonomous)** | Depends on the method above | Check, don't assume |

**Outcomes for the pipeline:**

- **Same SCN sequence** (in-place upgrade, Data Guard switchover): no change in Spark. Only GoldenGate needs repositioning.
- **Target SCN below the cursor:** every new transaction is dropped, with only a WARN logged, until the new database's SCN passes the old high-water mark. Data is lost and nothing fails.
- **Target SCN above the cursor:** it works, but only by luck. Treat it as a re-initialisation anyway unless it's the same SCN sequence.

**Cutover risk, whatever the method:** if the old and new Extract both publish for a while, data events and markers from two sources mix on the same topics. Duplicates are dropped by xid:csn, but counts from the two sides can overlap and block a transaction.

## Cutover procedure

1. **Drain the source.** Stop writes on the source, let `kc` and `rep3` catch up, and wait until the sequencer has no blocked head: the queue is empty or holds only completed transactions (see `STATE-STORE-DEBUG.md`). Stop the old Extract.
2. **Compare SCNs.** Note the last emitted CSN from the `cursor` state, then on the target run:
   ```sql
   SELECT current_scn FROM v$database;
   ```
3. **Same SCN sequence:** if the target is a physical standby on the same sequence and its SCN is above the cursor, reposition GoldenGate and continue with the same checkpoint.
4. **Otherwise, re-initialise:**
   - Create a new Extract, or register it again on the target.
   - Start new trails, and reposition `kc` and `rep3` to one shared cut point, as in the
     [go-live runbook](gg-configs/README.md#go-live-first-start-with-markers).
   - Start `TransactionalCdcProcessingApp` with a **new checkpoint location**, so the cursor starts empty.
   - Keep `order_stream` as history. `orders_current` stays correct as long as the migrated data starts with the same row versions.

The same re-initialisation applies after a **Flashback Database or point-in-time recovery with `RESETLOGS`**. In that case, also decide whether to truncate or reconcile `order_stream` and `orders_current`, because rows from the abandoned timeline are still in them.

## Suggested guard (not implemented)

The sequencer only logs a WARN for a marker below the cursor. A guard that **alerts or fails the query** when a marker's CSN is well below the last emitted CSN would turn silent data loss into a visible failure. A migration or `RESETLOGS` is exactly when it would fire.
