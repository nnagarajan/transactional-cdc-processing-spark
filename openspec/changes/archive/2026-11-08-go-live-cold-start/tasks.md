# Tasks

## 1. Kafka source options

- [x] 1.1 Add `KafkaSourceOptions` (main package): validate a starting position (`earliest`, `latest`, or a JSON map of partition to non-negative offset) and build the Spark options for one topic, wrapping a partition map as `{"<topic>":{...}}`; read `max-offsets-per-trigger`, where `none` means no limit. Verify with a new `KafkaSourceOptionsTest`: earliest, latest, partition map, fallback to the shared default, `{"0":"abc"}` and a negative offset both rejected with the topic key in the message, and `none` omitting the option
- [x] 1.2 Use `KafkaSourceOptions` in `TransactionalCdcProcessingApp.readKafkaStream` for all four sources, passing each topic's config key; log each source's starting position and limit at startup. Verify `./mvnw -Dquick clean package` compiles, and that a run with an invalid override fails before the query starts
- [x] 1.3 Add `job.kafka.max-offsets-per-trigger = 50000` to `reference.conf`, and a commented `job.kafka.starting-offsets` example block (one key per topic) to `transactional-cdc-processing.conf`. Verify the app starts with no overrides and logs `earliest` and 50000 for every source

## 2. Orphan-buffer purge

- [x] 2.1 Add `bufferKeys: Iterator[String]` to `SequencerStore`, implemented with `MapState.keys()` in `TransactionOrderingProcessor` and over the map in `InMemorySequencerStore`. Verify `./mvnw -Dquick clean package` compiles
- [x] 2.2 In `TransactionSequencer`, after `emitReady`, when `lastEmittedCsn` increased in the step: collect up to 100,000 keys whose CSN (text after the last `:`) is numerically below it, remove them after the scan, and log the count and the bound. Verify the existing `TransactionSequencerTest` and integration tests still pass
- [x] 2.3 Add `TransactionSequencerTest` cases for each `buffer-retention` scenario: history below the cursor removed, data ahead of its marker kept, same-CSN buffer kept, nothing removed before the first emission, incomplete head kept while a lower orphan is removed, and more than 100,000 orphans removed over several steps. Verify `./mvnw clean package` passes all tests
- [x] 2.4 Update `CLAUDE.md` Known Limitations, `IMPLEMENTATION.md` and `STATE-STORE-DEBUG.md`: buffers without a marker are now removed once the cursor passes their CSN, plus a query counting remaining buffers below the cursor. Verify `grep` finds no remaining "stay buffered indefinitely" wording

## 3. Go-live runbook

- [ ] 3.1 Add a "Go-live: first start with markers" section to `gg-configs/README.md`: create the 1-partition metadata topic; `STOP REPLICAT kc`; record `INFO REPLICAT kc, SHOWCH` and the data topics' end offsets (`kafka-get-offsets --time -1`), and check they don't change; `ALTER REPLICAT rep3, EXTSEQNO <n>, EXTRBA <rba>`; start `kc` and `rep3`; set the per-topic offsets, with the metadata topic at `earliest`; start the app on a new checkpoint; and check that the first marker's transaction reaches `order_stream` with no blocked alert. Add a fallback subsection for starting from `earliest` and relying on the purge. Verify every command against the local stack's GGSCI and Kafka tools
- [x] 3.2 Link the runbook from `SCN-CHANGES-AND-MIGRATION.md`'s cutover procedure and from `CLAUDE.md`. Verify the links resolve

## 4. Integration

- [ ] 4.1 Rehearse the runbook locally: with history in the data topics and a recreated, empty metadata topic, follow 3.1, commit a few transactions in Oracle, and verify they all reach `order_stream`, no blocked alert is logged, and the `buffers` state holds no pre-go-live transactions
- [ ] 4.2 Rehearse the fallback: start a new checkpoint from `earliest` with `max-offsets-per-trigger` set low (for example 100), commit a transaction, and verify that query progress shows no batch above the limit per source, that the purge log reports the history removed after the first emission, and that the `buffers` state count drops to the in-flight transactions only
