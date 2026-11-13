# Spec Delta

## MODIFIED Requirements

### Requirement: Per-topic starting position
On a new checkpoint, each of a job's Kafka sources SHALL start at its own configured position if one is set, and otherwise at the shared default position. The sources are one per entity in the model, plus the transaction metadata source. Positions SHALL be keyed by entity name, or by `transaction-metadata`. A position SHALL be `earliest`, `latest`, or an explicit offset for each partition of that topic.

#### Scenario: Explicit offsets for the data topics
- **WHEN** the orders job's three entity topics are configured with explicit partition offsets and the metadata topic has no override, with the default `earliest`
- **THEN** each entity topic's first batch starts at its configured offsets, and the metadata topic starts at its earliest offset

#### Scenario: No overrides
- **WHEN** no per-topic position is configured
- **THEN** every source starts at the shared default position

#### Scenario: Existing checkpoint
- **WHEN** the pipeline restarts on an existing checkpoint with different configured positions
- **THEN** every source resumes from the checkpointed offsets, and the configured positions are ignored

#### Scenario: Position for an unknown entity
- **WHEN** a starting position is configured under a key that is neither an entity of the job nor `transaction-metadata`
- **THEN** the job fails before any query starts, naming that key
