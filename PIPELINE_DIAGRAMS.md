# Pipeline Architecture Diagrams

Mermaid diagrams of both CDC processing pipelines. Tables are Unity Catalog managed Delta tables
in `mymanaged.argus1`, running on Spark 4.1.1 and Delta 4.2.0 (see [UNITY_CATALOG.md](UNITY_CATALOG.md)).

## Pipeline 1: SCD Type 2, Ordered Transaction Assembly

`TransactionalCdcProcessingApp` reads CDC events from Kafka and assembles each source transaction
in a single `transformWithState` sequencer. It writes complete transactions to the SCD Type 2 table
`argus1.order_stream`, strictly in source commit order.

```mermaid
flowchart TD
    %% Data Sources
    K1[(Kafka Topic<br/>dev.appuser.orders.json)]
    K2[(Kafka Topic<br/>dev.appuser.order_details.json)]
    K3[(Kafka Topic<br/>dev.appuser.order_line_items.json)]
    K4[("Kafka Topic<br/>dev.transaction_metadata_json<br/>exactly 1 partition")]

    %% Streaming Reads
    K1 --> SR1[Stream Read<br/>Parse JSON]
    K2 --> SR2[Stream Read<br/>Parse JSON]
    K3 --> SR3[Stream Read<br/>Parse JSON]
    K4 --> SR4["Stream Read<br/>Parse JSON<br/>keep Kafka partition + offset"]

    %% Union
    SR1 --> U[Union All Streams]
    SR2 --> U
    SR3 --> U
    SR4 --> U

    %% Filter and single key
    U --> F{xid and csn<br/>not null?}
    F -->|No| D1[Discard Record]
    F -->|Yes| G["groupByKey<br/>one global key: all"]

    %% Sequencer
    G --> SP["transformWithState<br/>TransactionOrderingProcessor<br/>processing-time mode"]
    SP <--> RS[("RocksDB State<br/>buffers: xid:csn to TransactionState<br/>queue: seq to xid:csn<br/>cursor: Cursor")]

    %% Per-batch input handling
    SP --> IN{Input row}
    IN -->|Marker| PART{"From partition 0?"}
    PART -->|No| FAIL[Fail the query<br/>metadata topic must<br/>have one partition]
    PART -->|Yes| MREP{"Already emitted<br/>or already queued?"}
    MREP -->|Yes| MDROP[Ignore marker]
    MREP -->|No| ENQ["Attach counts to buffer<br/>append xid:csn to queue<br/>in Kafka offset order"]

    IN -->|Data event| DREP{"Transaction<br/>already emitted?"}
    DREP -->|Yes| DDROP[Drop event]
    DREP -->|No| DUP{"Same pos already<br/>buffered?"}
    DUP -->|Yes| DDROP2[Ignore duplicate]
    DUP -->|No| BUF[Buffer event<br/>by table]

    %% Emission walk
    ENQ --> WALK
    BUF --> WALK
    WALK{"Queue head complete?<br/>exact per-table counts"}
    WALK -->|Yes| JOIN["OrderJoiner.joinTransaction<br/>group by ORDER_ID<br/>nested arrays with before images"]
    JOIN --> REC["Remove from buffers and queue<br/>advance cursor"]
    REC --> WALK
    WALK -->|"No, or queue empty"| BLK["Track blocked time<br/>ERROR log once blocked longer than<br/>job.ordering.blocked-alert-threshold"]

    %% Output
    JOIN --> DL1[("argus1.order_stream<br/>SCD Type 2<br/>UC managed Delta table")]

    %% Timer and checkpoint
    TMR["Processing-time timer<br/>job.ordering.check-interval"] -.-> WALK
    SP -.->|Checkpointing| CP1[("Checkpoint<br/>Kafka offsets + RocksDB state")]

    classDef kafka fill:#ff9,stroke:#333,stroke-width:2px
    classDef process fill:#9cf,stroke:#333,stroke-width:2px
    classDef state fill:#fcf,stroke:#333,stroke-width:2px
    classDef decision fill:#ffa,stroke:#333,stroke-width:2px
    classDef output fill:#9f9,stroke:#333,stroke-width:2px
    classDef skip fill:#ddd,stroke:#333,stroke-width:2px
    classDef fail fill:#f99,stroke:#333,stroke-width:2px

    class K1,K2,K3,K4 kafka
    class SR1,SR2,SR3,SR4,U,G,SP,ENQ,BUF,JOIN,REC,BLK,TMR process
    class RS,CP1 state
    class F,IN,PART,MREP,DREP,DUP,WALK decision
    class DL1 output
    class D1,MDROP,DDROP,DDROP2 skip
    class FAIL fail
```

### Key Components

| Component | Description |
|-----------|-------------|
| **Kafka Topics** | 3 data topics plus the transaction metadata (marker) topic. The metadata topic must have exactly one partition |
| **Stream Processing** | Parse JSON. Markers keep their Kafka partition and offset |
| **Union + Filter** | Combine all 4 streams; drop rows without `xid`/`csn` |
| **Global key** | All rows share the key `all`, so one sequencer sees every in-flight transaction. It runs as a single task |
| **TransactionOrderingProcessor** | Spark `StatefulProcessor` adapter over `TransactionSequencer`, which holds the logic and is unit-tested without Spark |
| **`buffers`** | `MapState`: `xid:csn` → `TransactionState` (events by table, deduplicated on `pos`, expected counts from the marker) |
| **`queue`** | `MapState`: sequence number → `xid:csn`, in marker (Kafka offset) order, which is commit order |
| **`cursor`** | `ValueState[Cursor]`: next sequence numbers, last emitted CSN and the xids emitted at it, blocked-since time, timer |
| **Emission walk** | After each batch and on each timer, emit from the head of the queue while the head is complete. An incomplete head blocks everything behind it and is never skipped |
| **Replay detection** | Events and markers whose CSN is below the last emitted CSN, or equal to it with an already-emitted xid, are dropped |
| **OrderJoiner** | Denormalize a complete transaction into `OrderStream` records |
| **Trigger** | `Trigger.ProcessingTime(job.ordering.trigger-interval)`. In processing-time mode `transformWithState` would otherwise run empty batches back to back |
| **Output** | `argus1.order_stream`, a UC managed Delta table. Commits go through the Unity Catalog server |

### Out-of-Order Completion

Transactions are emitted in marker order, not in the order they complete:

```mermaid
sequenceDiagram
    participant K as Kafka topics
    participant S as Sequencer
    participant Q as queue (state)
    participant D as order_stream

    K->>S: marker T1 (offset 10)
    S->>Q: seq 0 = T1
    K->>S: marker T2 (offset 11)
    S->>Q: seq 1 = T2
    K->>S: all data events of T2
    Note over S: T2 complete, but head T1 is not
    Note over S: T2 stays buffered
    K->>S: last data event of T1
    Note over S: head T1 complete
    S->>D: emit T1
    S->>D: emit T2 (same batch)
    S->>Q: remove seq 0 and 1, cursor = T2's CSN
```

To inspect `buffers`, `queue` and `cursor` from a checkpoint, see
[STATE-STORE-DEBUG.md](STATE-STORE-DEBUG.md).

---

## Pipeline 2: SCD Type 1, Current State Maintenance

`ScdType1MergeApp` streams `argus1.order_stream` and runs `ScdType1Merger` in `foreachBatch`. The
merger keeps one row per order in `argus1.orders_current`, versioning each entity independently.

```mermaid
flowchart TD
    DL1[("argus1.order_stream<br/>SCD Type 2<br/>All History")]

    DL1 --> SR[Stream Read<br/>Delta Format]
    SR --> FB[foreachBatch<br/>ScdType1Merger.mergeScdType1]

    %% Transform
    FB --> TR["Flatten batch to orders_current shape<br/>one row per orderId"]
    TR --> DED["Dedup within the batch<br/>order: highest version, ties broken by higher CSN<br/>orderDetails: highest version<br/>lineItems: highest version per lineItemId"]

    %% Target check
    DED --> CHK{Target table<br/>exists?}
    CHK -->|No| INIT[Initial write<br/>saveAsTable]
    INIT --> DL2
    CHK -->|Yes| MERGE[Delta MERGE<br/>on orderId]

    MERGE --> MATCH{Matched?}

    %% Not matched
    MATCH -->|No| INS["INSERT row<br/>child-only change becomes a<br/>placeholder with null order fields"]
    INS --> DL2

    %% Matched
    MATCH -->|Yes| VER{"source.version ><br/>target.version?"}
    VER -->|Yes| UPD1[Take all order fields<br/>from source]
    VER -->|"No, or source has<br/>no order record"| UPD2[Keep target order fields]

    UPD1 --> TX
    UPD2 --> TX
    TX{"source CSN ><br/>target CSN?"}
    TX -->|Yes| TXU["Update xid, csn,<br/>dwhProcessedTs"]
    TX -->|No| TXK[Keep transaction context<br/>never moves to a lower CSN]

    TXU --> ENT
    TXK --> ENT
    ENT[Entity-level child merge]
    ENT --> DET{"orderDetails:<br/>source version higher?"}
    DET -->|Yes| DETUPD[Take source detail]
    DET -->|No| DETKEEP[Keep target detail]
    DETUPD --> LI
    DETKEEP --> LI
    LI{"Each line item, by lineItemId:<br/>source version higher?"}
    LI -->|Yes| LIUPD[Take source item]
    LI -->|No| LIKEEP[Keep target item]
    LIUPD --> DL2
    LIKEEP --> DL2

    DL2[("argus1.orders_current<br/>SCD Type 1<br/>UC managed Delta table")]

    FB -.->|Checkpointing| CP2[(Checkpoint<br/>Delta source offsets)]

    classDef source fill:#ff9,stroke:#333,stroke-width:2px
    classDef process fill:#9cf,stroke:#333,stroke-width:2px
    classDef state fill:#fcf,stroke:#333,stroke-width:2px
    classDef decision fill:#ffa,stroke:#333,stroke-width:2px
    classDef output fill:#9f9,stroke:#333,stroke-width:2px
    classDef skip fill:#ddd,stroke:#333,stroke-width:2px

    class DL1 source
    class SR,FB,TR,DED,INIT,MERGE,INS,UPD1,UPD2,TXU,ENT,DETUPD,LIUPD process
    class CP2 state
    class CHK,MATCH,VER,TX,DET,LI decision
    class DL2 output
    class TXK,DETKEEP,LIKEEP skip
```

### Key Components

| Component | Description |
|-----------|-------------|
| **Source** | `argus1.order_stream`, read as a Delta stream |
| **ScdType1Merger** | Extracted from `ScdType1MergeApp`; transforms each batch and runs the MERGE. Tested in `ScdType1MergerTest` |
| **Intra-batch dedup** | One row per `orderId`. Order: highest `version`, ties broken by higher CSN (numeric). Children deduplicated by their own version |
| **Delta MERGE** | Matches on `orderId`. Two `whenMatched` clauses: source order version wins, or keep the target's order fields |
| **Transaction context guard** | `xid`, `csn`, `dwhProcessedTs` update only when the source CSN is numerically higher, so a replay never moves them backwards |
| **Placeholders** | An order first seen through a child-only change is inserted with null order fields and filled in when its order record arrives |
| **Entity-level merge** | Order, order details and each line item are versioned independently |
| **Output** | `argus1.orders_current`, a UC managed Delta table |

---

## Complete End-to-End Flow

```mermaid
flowchart LR
    subgraph Oracle["Oracle + GoldenGate"]
        direction TB
        DB[(Oracle tables<br/>ORDERS, ORDER_DETAILS,<br/>ORDER_LINE_ITEMS)]
        EXT["Extract ext04<br/>IGNOREDELETES"]
        RK[Replicat kc<br/>data topics]
        R3["Replicat rep3<br/>markers, blockingSend=true<br/>acks=all, idempotent"]
        DB --> EXT
        EXT --> RK
        EXT --> R3
    end

    subgraph Kafka["Kafka Cluster"]
        K1[orders.json]
        K2[order_details.json]
        K3[order_line_items.json]
        K4["transaction_metadata_json<br/>1 partition"]
    end

    subgraph P1["Pipeline 1: SCD Type 2<br/>TransactionalCdcProcessingApp"]
        direction TB
        READ1[Read & Parse<br/>4 Topics]
        SEQ["Ordered sequencer<br/>transformWithState<br/>RocksDB state"]
        JOIN1[OrderJoiner]
        READ1 --> SEQ --> JOIN1
    end

    DL1[("argus1.order_stream<br/>SCD Type 2")]

    subgraph P2["Pipeline 2: SCD Type 1<br/>ScdType1MergeApp"]
        direction TB
        READ2[Stream Read<br/>Delta]
        MERGE["ScdType1Merger<br/>version-aware MERGE"]
        READ2 --> MERGE
    end

    DL2[("argus1.orders_current<br/>SCD Type 1")]

    UC[["Unity Catalog server :7071<br/>catalog mymanaged<br/>metadata + managed commits"]]

    subgraph Analytics["Analytics & Queries"]
        direction TB
        Q1[Historical Analysis<br/>Time Travel<br/>Audit Trail]
        Q2[Current State<br/>Operational Queries]
    end

    RK --> K1
    RK --> K2
    RK --> K3
    R3 --> K4
    Kafka --> P1
    P1 --> DL1
    DL1 --> P2
    P2 --> DL2
    DL1 -.- UC
    DL2 -.- UC
    DL1 -.-> Q1
    DL2 -.-> Q2

    classDef source fill:#fdb,stroke:#333,stroke-width:3px
    classDef kafka fill:#ff9,stroke:#333,stroke-width:3px
    classDef pipeline fill:#9cf,stroke:#333,stroke-width:3px
    classDef storage fill:#9f9,stroke:#333,stroke-width:3px
    classDef catalog fill:#ccf,stroke:#333,stroke-width:3px
    classDef analytics fill:#fcc,stroke:#333,stroke-width:3px

    class Oracle source
    class Kafka kafka
    class P1,P2 pipeline
    class DL1,DL2 storage
    class UC catalog
    class Analytics analytics
```

Deletes are filtered at the Extract, so the data topics and the marker counts always agree. The
apps resolve `argus1.*` against the default catalog `mymanaged`. Every commit to a managed table goes
through the Unity Catalog server, so the server must be running while the apps write.

---

## Data Model Comparison

```mermaid
erDiagram
    ORDER_STREAM {
        string xid PK
        string csn PK
        double orderId PK
        string dwhProcessedTs
        array_struct orders
        array_struct orderDetails
        array_struct lineItems
    }

    ORDERS_CURRENT {
        double orderId PK
        string xid
        string csn
        string dwhProcessedTs
        double version
        string orderRef
        struct orderBefore
        struct orderDetails
        array_struct lineItems
    }

    ORDER_STREAM ||--o{ ORDERS_CURRENT : "latest version"
```

### Schema Differences

| Aspect | SCD Type 2 (`order_stream`) | SCD Type 1 (`orders_current`) |
|--------|---------------------------|-------------------------------|
| **Primary Key** | (xid, csn, orderId), composite | orderId |
| **Row order** | Commits in source commit order | n/a, one row per order |
| **Order Structure** | Nested `orders ARRAY<STRUCT>` | Flattened top-level fields (null for a placeholder) |
| **Order Details** | `ARRAY<STRUCT>` with before | Single `STRUCT` with before (1:1) |
| **Line Items** | `ARRAY<STRUCT>` with before | `ARRAY<STRUCT>` with before (1:N) |
| **Records per Order** | One per transaction that touched it | One (current only) |
| **Inserts** | Always append | Upsert based on version |
| **Updates** | Never update, only append | Update in place if version is higher |
| **Transaction context** | The transaction that produced the row | Highest CSN seen for the order |
| **Before Images** | Inside each array element | `orderBefore` struct + inside details/items |
| **Use Case** | Historical analysis, audit | Current state queries |

---

## Processing Guarantees

```mermaid
flowchart TD
    subgraph G1["Transaction Guarantees"]
        T1[All events of a transaction<br/>emitted together]
        T2[No partial transactions:<br/>exact per-table counts,<br/>dedup on pos]
        T3[Deletes filtered at the Extract,<br/>so counts and topics agree]
        T1 --> T2 --> T3
    end

    subgraph G0["Ordering Guarantees"]
        O1[Emission in marker order<br/>= source commit order]
        O2[Incomplete head blocks output,<br/>never skipped; alerted]
        O3[Replays of emitted transactions<br/>dropped, across restarts]
        O1 --> O2 --> O3
    end

    subgraph G2["Versioning Guarantees"]
        V1[Entity-level version tracking]
        V2[Transaction context never<br/>moves to a lower CSN]
        V3[No version regression,<br/>always keep latest]
        V1 --> V2 --> V3
    end

    subgraph G3["ACID Guarantees"]
        A1[Atomic writes<br/>Delta Lake]
        A2[Consistent reads<br/>Snapshot isolation]
        A3[Durable state<br/>RocksDB + Checkpoints]
        A1 --> A2 --> A3
    end

    G1 --> G0 --> G2 --> G3

    classDef guarantee fill:#9f9,stroke:#333,stroke-width:2px
    class G0,G1,G2,G3 guarantee
```

### What These Guarantees Cost

- **Head-of-line blocking:** a transaction that never completes (lost event or marker) halts all
  output until someone intervenes. It's logged as an error, never skipped.
- **No scale-out:** the sequencer runs as a single task on one global key.
- **Unbounded orphan buffers:** data events whose marker never arrives stay buffered. They can't
  block output, but they consume state (no TTL).
- **Single-partition metadata topic:** markers must arrive in commit order on one partition.
