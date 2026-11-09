# buffer-retention Specification

## Purpose

Defines when buffered data for a transaction that can never be emitted is removed from state, so data without a marker (such as history from before markers existed) does not stay in state forever.

## Requirements

### Requirement: Unemittable buffers are removed
Once a transaction has been emitted at CSN C, the system SHALL remove every buffered transaction that has no marker and whose CSN is numerically below C. Markers arrive in commit order, so such a transaction can never receive a marker that would be accepted.

#### Scenario: History from before markers existed
- **WHEN** data events for 1,000 transactions with CSNs 100 to 1,099 are buffered without markers, and then a transaction at CSN 2,000 is emitted
- **THEN** all 1,000 buffers are removed from state

#### Scenario: Data that arrived ahead of its marker
- **WHEN** a transaction at CSN 500 is emitted, and data for a transaction at CSN 600 is buffered without its marker yet
- **THEN** the buffer at CSN 600 is kept, and that transaction is emitted once its marker arrives and it is complete

#### Scenario: Same CSN as the last emitted transaction
- **WHEN** transaction A at CSN 500 is emitted, and data for transaction B at CSN 500 is buffered without its marker
- **THEN** B's buffer is kept, because B's marker may still follow A's

#### Scenario: Nothing emitted yet
- **WHEN** data events are buffered without markers and no transaction has been emitted since the checkpoint was created
- **THEN** no buffer is removed

### Requirement: Queued transactions are never removed
The removal SHALL NOT remove a transaction whose marker has been accepted, whether it is complete or not. Emission order and the rule that an incomplete head is never skipped SHALL be unchanged.

#### Scenario: Incomplete head with a lower CSN buffer present
- **WHEN** the head-of-line transaction at CSN 700 is incomplete, and an unmarked buffer at CSN 300 exists, with 500 as the last emitted CSN
- **THEN** the buffer at CSN 300 is removed, and the head at CSN 700 stays queued and still blocks later transactions

### Requirement: Removals are logged
Each time buffers are removed, the system SHALL log how many were removed and the CSN below which they were removed.

#### Scenario: Removal logged
- **WHEN** 1,000 unemittable buffers are removed after a transaction at CSN 2,000 is emitted
- **THEN** a log entry reports 1,000 buffers removed below CSN 2,000
