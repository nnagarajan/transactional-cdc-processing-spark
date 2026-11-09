# Tasks

## 1. Formatter module

- [x] 1.1 Create `gg-formatter/` with `pom.xml` (system-scoped GoldenGate 21.4 jars via `lib.location`, JUnit 4.13, Mockito 3.12.4) and `TransactionMetaDataType`, copied from `~/github/goldengate/gg-custom-formatters`; verify `./mvnw -f gg-formatter/pom.xml clean test` builds
- [x] 1.2 Rewrite `TransactionBoundaryEventJsonFormatter` to group operations by their own xid:csn (`LinkedHashMap`, trail order, falling back to the delivery's ids when null), writing a single object for one group and a JSON array for several; verify the module builds
- [x] 1.3 Add `TransactionBoundaryEventJsonFormatterTest` (8 tests: single object, per-table counts, three grouped transactions, per-transaction timestamps, same CSN with different xids, 50 grouped transactions whose counts add up, null-id fallback, only the last operation writes); verify all 8 pass
- [x] 1.4 Run the same tests against the original formatter in a scratch copy; verify the 4 grouped-delivery tests fail and the 4 single-transaction tests pass
- [x] 1.5 Write `gg-formatter/README.md` (contract, handler behaviour behind it, both message forms, build and deploy); verify its build command runs as written

## 2. Pipeline parsing and ordering

- [x] 2.1 Move topic parsing from `TransactionalCdcProcessingApp` into `CdcJsonParsing`. Parse metadata with `posexplode(from_json(json, 'array<struct<…>>'))`, add `marker_index` (null for data events); verify `./mvnw -Dquick clean package` compiles
- [x] 2.2 Add `index: Int = 0` to `MarkerInput`, read `marker_index` in `CdcRows` (0 when absent or null), and sort markers by `(offset, index)` in `TransactionSequencer`; verify the existing `TransactionSequencerTest` and integration tests still pass
- [x] 2.3 Add `CdcJsonParsingTest` (the real single marker, a 3-marker array, and a mix of both, checking xid, csn, counts, offset and index) and `TransactionSequencerTest.markersSharingAMessageKeepTheirOrderWithinIt`; verify `./mvnw clean package` passes all 37 tests

## 3. Documentation

- [x] 3.1 Document the marker contract and the no-`MAXTRANSOPS` rule in `gg-configs/README.md`, add the formatter row to the source contract in `IMPLEMENTATION.md`, and add `CdcJsonParsing` and the marker note to `CLAUDE.md`; verify each file references `gg-formatter/`

## 4. Deployment and live verification

- [ ] 4.1 Build `gg-formatter`, copy the jar to `/opt/oracle/ggbd/ggjava/resources/customlib/`, and restart `rep3`; verify `REP3.log` shows the formatter instantiated after the restart and the deployed jar's checksum matches the new build
- [ ] 4.2 Live grouping test: stop `rep3`, commit 3–5 small transactions in Oracle (including two at the same moment), start `rep3`; verify `dev.transaction_metadata_json` holds one marker per xid:csn, with counts matching the data topics, and the pipeline emits every transaction
- [ ] 4.3 Resolve the open question on `GROUPTRANSOPS 1` in `rep3.prm`, and record the decision in `gg-configs/README.md`; verify `rep3.prm` in the repo and in `/opt/oracle/ggbd/dirprm/` match
