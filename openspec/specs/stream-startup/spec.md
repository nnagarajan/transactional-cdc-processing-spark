# stream-startup Specification

## Purpose

Defines where each Kafka source starts reading when the pipeline starts on a new checkpoint, and how much each source reads per micro-batch, so a first start or a backlog never produces one unbounded batch.

## Requirements

### Requirement: Per-topic starting position
On a new checkpoint, each of the four Kafka sources (orders, order details, order line items, transaction metadata) SHALL start at its own configured position if one is set, and otherwise at the shared default position. A position SHALL be `earliest`, `latest`, or an explicit offset for each partition of that topic.

#### Scenario: Explicit offsets for the data topics
- **WHEN** the three data topics are configured with explicit partition offsets and the metadata topic has no override, with the default `earliest`
- **THEN** each data topic's first batch starts at its configured offsets, and the metadata topic starts at its earliest offset

#### Scenario: No overrides
- **WHEN** no per-topic position is configured
- **THEN** every source starts at the shared default position, as before this change

#### Scenario: Existing checkpoint
- **WHEN** the pipeline restarts on an existing checkpoint with different configured positions
- **THEN** every source resumes from the checkpointed offsets, and the configured positions are ignored

### Requirement: Invalid starting position fails at startup
The pipeline SHALL refuse to start, naming the topic and the value, when a configured starting position is not `earliest`, `latest`, or a valid map of partition numbers to non-negative offsets.

#### Scenario: Malformed offsets
- **WHEN** the orders topic is configured with the position `{"0":"abc"}`
- **THEN** the pipeline fails before the query starts, with an error naming the orders topic and the value

### Requirement: Bounded offsets per micro-batch
Each Kafka source SHALL read at most a configured number of offsets per micro-batch. A default limit SHALL apply when none is configured, and the limit SHALL be possible to remove.

#### Scenario: First start from earliest with a large history
- **WHEN** a data topic holds 1,000,000 messages, the pipeline starts from `earliest`, and the limit is 50,000
- **THEN** no micro-batch reads more than 50,000 offsets from that topic, and the history is read over successive batches

#### Scenario: Transaction split across batches
- **WHEN** the limit splits one transaction's data events across two micro-batches
- **THEN** the transaction is emitted once all its events have arrived, with the same output as if they had arrived in one batch
