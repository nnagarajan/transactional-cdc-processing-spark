# Pipeline Architecture Diagrams

This document contains Mermaid diagrams illustrating the architecture of both CDC processing pipelines.

## Pipeline 1: SCD Type 2 - Transactional CDC Processing

This pipeline reads CDC events from Kafka, performs transaction-aware buffering, and writes to an SCD Type 2 Delta Lake table with full history.

```mermaid
flowchart TD
    %% Data Sources
    K1[(Kafka Topic<br/>dev.appuser.orders.json)]
    K2[(Kafka Topic<br/>dev.appuser.order_details.json)]
    K3[(Kafka Topic<br/>dev.appuser.order_line_items.json)]
    K4[(Kafka Topic<br/>dev.transaction_metadata_json)]

    %% Streaming Reads
    K1 --> SR1[Stream Read<br/>Parse JSON]
    K2 --> SR2[Stream Read<br/>Parse JSON]
    K3 --> SR3[Stream Read<br/>Parse JSON]
    K4 --> SR4[Stream Read<br/>Parse JSON]

    %% Add Event Type
    SR1 --> ET1[Add Event Type<br/>ORDERS]
    SR2 --> ET2[Add Event Type<br/>ORDER_DETAILS]
    SR3 --> ET3[Add Event Type<br/>ORDER_LINE_ITEMS]
    SR4 --> ET4[Add Event Type<br/>METADATA]

    %% Union
    ET1 --> U[Union All Streams]
    ET2 --> U
    ET3 --> U
    ET4 --> U

    %% Filter
    U --> F{Filter<br/>xid & csn<br/>not null?}
    F -->|Yes| G[Group By<br/>Transaction Key<br/>xid:csn]
    F -->|No| D1[Discard Record]

    %% State Processing
    G --> SP[flatMapGroupsWithState<br/>TransactionBufferingProcessor]
    SP --> RS[(RocksDB State<br/>TransactionState)]
    RS --> SP

    %% Transaction Logic
    SP --> TL{Transaction<br/>Complete?}

    %% Check Metadata
    TL -->|Check| MD{Metadata<br/>Received?}
    MD -->|No| BUF1[Buffer Events<br/>Update State]
    BUF1 --> RT1[Return Empty<br/>No Emission]

    %% Check Event Counts
    MD -->|Yes| EC{Event Counts<br/>Match?}
    EC -->|No| BUF2[Buffer Events<br/>Update State]
    BUF2 --> RT2[Return Empty<br/>No Emission]

    %% Complete Transaction
    EC -->|Yes| JOIN[OrderJoiner.joinTransaction<br/>Group by ORDER_ID]
    JOIN --> NEST[Build Nested Arrays<br/>orders, orderDetails, lineItems<br/>Each with before images]
    NEST --> REM[Remove State<br/>state.remove]
    REM --> EMIT[Emit Denormalized<br/>OrderStream]

    %% Output
    EMIT --> DL1[(Delta Lake Table<br/>order_stream<br/>SCD Type 2)]

    %% Checkpoint
    SP -.->|Checkpointing| CP1[(Checkpoint<br/>RocksDB State Store)]

    %% Styling
    classDef kafka fill:#ff9,stroke:#333,stroke-width:2px
    classDef process fill:#9cf,stroke:#333,stroke-width:2px
    classDef state fill:#fcf,stroke:#333,stroke-width:2px
    classDef decision fill:#ffa,stroke:#333,stroke-width:2px
    classDef output fill:#9f9,stroke:#333,stroke-width:2px

    class K1,K2,K3,K4 kafka
    class SR1,SR2,SR3,SR4,ET1,ET2,ET3,ET4,U,G,SP,JOIN,FLAT,VER kafka,process
    class RS,CP1 state
    class F,TL,MD,EC decision
    class DL1 output
```

### Key Components

| Component | Description |
|-----------|-------------|
| **Kafka Topics** | 4 source topics for orders, order_details, order_line_items, and transaction metadata |
| **Stream Processing** | Parse JSON and add event type markers |
| **Union** | Combine all 4 streams into single stream |
| **Filter** | Remove records without transaction identifiers |
| **Group By** | Group events by transaction key (xid:csn) |
| **State Management** | RocksDB-backed stateful processing with TransactionState |
| **Transaction Logic** | Buffer until metadata received and event counts match |
| **OrderJoiner** | Join all events by ORDER_ID, flatten attributes, add before images |
| **Delta Lake Output** | Write denormalized records to SCD Type 2 table |

---

## Pipeline 2: SCD Type 1 - Current State Maintenance

This pipeline reads from the SCD Type 2 table and maintains an SCD Type 1 table with only the latest version of each order using entity-level versioning.

```mermaid
flowchart TD
    %% Source
    DL1[(Delta Lake Table<br/>order_stream<br/>SCD Type 2<br/>All History)]

    %% Stream Read
    DL1 --> SR[Stream Read<br/>Delta Format]

    %% Micro-batch Processing
    SR --> FB[foreachBatch<br/>Batch Processing]
    FB --> BATCH{For Each<br/>Micro-batch}

    %% Check Target
    BATCH --> CHK{Target Table<br/>Exists?}
    CHK -->|No| INIT[Initial Write<br/>mode=overwrite]
    INIT --> DL2
    CHK -->|Yes| MERGE[Delta Lake MERGE<br/>Operation]

    %% Merge Logic
    MERGE --> MATCH{Match on<br/>ORDER_ID?}

    %% Not Matched - Insert
    MATCH -->|No Match| INS[INSERT<br/>New Order]
    INS --> DL2

    %% Matched - Check Versions
    MATCH -->|Matched| VER{Version<br/>Comparison}

    %% Order Version Check
    VER -->|source.VERSION ><br/>target.VERSION| UPD1[UPDATE<br/>All Order Fields<br/>Update Arrays]

    %% Timestamp Tiebreaker
    VER -->|source.VERSION =<br/>target.VERSION| TS{dwh_processed_ts<br/>Comparison}
    TS -->|source.ts ><br/>target.ts| UPD2[UPDATE<br/>Keep Latest]
    TS -->|source.ts <=<br/>target.ts| SKIP1[Skip Update<br/>Duplicate]

    %% Version Lower
    VER -->|source.VERSION <<br/>target.VERSION| SKIP2[Skip Update<br/>Older Version]

    %% Entity-Level Updates
    UPD1 --> ENT[Entity-Level Merge<br/>for Details & Line Items]
    UPD2 --> ENT

    %% Detail Merge
    ENT --> DET{For Each<br/>Order Detail}
    DET --> DETVER{Detail<br/>VERSION<br/>by ORDER_ID}
    DETVER -->|Higher| DETUPD[Update Detail]
    DETVER -->|Same/Lower| DETKEEP[Keep Existing]
    DETUPD --> LI
    DETKEEP --> LI

    %% Line Item Merge
    LI{For Each<br/>Line Item}
    LI --> LIVER{Line Item<br/>VERSION<br/>by LINE_ITEM_ID}
    LIVER -->|Higher| LIUPD[Update Line Item]
    LIVER -->|Same/Lower| LIKEEP[Keep Existing]
    LIUPD --> DL2
    LIKEEP --> DL2

    %% Output
    DL2[(Delta Lake Table<br/>orders_current<br/>SCD Type 1<br/>Current State Only)]

    %% Checkpoint
    FB -.->|Checkpointing| CP2[(Checkpoint<br/>Streaming State)]

    %% Monitoring
    DL2 -.->|CDC Feed| CDF[Change Data Feed<br/>enabled]
    DL2 -.->|Validation| VAL{Uniqueness<br/>Check<br/>ORDER_ID}

    %% Styling
    classDef source fill:#ff9,stroke:#333,stroke-width:2px
    classDef process fill:#9cf,stroke:#333,stroke-width:2px
    classDef state fill:#fcf,stroke:#333,stroke-width:2px
    classDef decision fill:#ffa,stroke:#333,stroke-width:2px
    classDef output fill:#9f9,stroke:#333,stroke-width:2px
    classDef skip fill:#ddd,stroke:#333,stroke-width:2px

    class DL1 source
    class SR,FB,INIT,MERGE,INS,UPD1,UPD2,ENT,DETUPD,DETKEEP,LIUPD,LIKEEP process
    class CP2 state
    class BATCH,CHK,MATCH,VER,TS,DET,DETVER,LI,LIVER,VAL decision
    class DL2 output
    class SKIP1,SKIP2 skip
```

### Key Components

| Component | Description |
|-----------|-------------|
| **Source** | Delta Lake SCD Type 2 table with full history |
| **Stream Read** | Continuous reading from Delta Lake in streaming mode |
| **foreachBatch** | Micro-batch processing for complex merge logic |
| **Delta MERGE** | ACID merge operation based on ORDER_ID |
| **Version Comparison** | Compare VERSION fields at entity level |
| **Entity-Level Merge** | Independent version tracking for order, order details, line items |
| **Duplicate Handling** | Skip records with same version using timestamp tiebreaker |
| **SCD Type 1 Output** | Maintain only current state of each order |
| **Change Data Feed** | Track changes to current state |

---

## Complete End-to-End Flow

```mermaid
flowchart LR
    %% Kafka Sources
    subgraph Kafka["Kafka Cluster"]
        K1[orders.json]
        K2[order_details.json]
        K3[order_line_items.json]
        K4[transaction_metadata.json]
    end

    %% Pipeline 1
    subgraph P1["Pipeline 1: SCD Type 2<br/>TransactionalCdcProcessingApp"]
        direction TB
        READ1[Read & Parse<br/>4 Topics]
        BUFFER[Transaction-Aware<br/>Buffering<br/>RocksDB State]
        JOIN1[Join & Flatten<br/>OrderJoiner]
        READ1 --> BUFFER --> JOIN1
    end

    %% SCD Type 2 Storage
    DL1[(order_stream<br/>SCD Type 2<br/>Full History)]

    %% Pipeline 2
    subgraph P2["Pipeline 2: SCD Type 1<br/>ScdType1MergeApp"]
        direction TB
        READ2[Stream Read<br/>Delta Lake]
        MERGE[Version-Aware<br/>MERGE<br/>Entity-Level]
        READ2 --> MERGE
    end

    %% SCD Type 1 Storage
    DL2[(orders_current<br/>SCD Type 1<br/>Current State)]

    %% Analytics
    subgraph Analytics["Analytics & Queries"]
        direction TB
        Q1[Historical Analysis<br/>Time Travel<br/>Audit Trail]
        Q2[Current State<br/>Operational Queries<br/>Dashboards]
    end

    %% Flow
    Kafka --> P1
    P1 --> DL1
    DL1 --> P2
    P2 --> DL2
    DL1 -.-> Q1
    DL2 -.-> Q2

    %% Styling
    classDef kafka fill:#ff9,stroke:#333,stroke-width:3px
    classDef pipeline fill:#9cf,stroke:#333,stroke-width:3px
    classDef storage fill:#9f9,stroke:#333,stroke-width:3px
    classDef analytics fill:#fcc,stroke:#333,stroke-width:3px

    class Kafka kafka
    class P1,P2 pipeline
    class DL1,DL2 storage
    class Analytics analytics
```

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

| Aspect | SCD Type 2 (order_stream) | SCD Type 1 (orders_current) |
|--------|---------------------------|-------------------------------|
| **Primary Key** | (xid, csn, orderId) - Composite | orderId - Single |
| **Order Structure** | Nested `orders ARRAY<STRUCT>` | Flattened top-level fields |
| **Order Details** | `ARRAY<STRUCT>` with before | Single `STRUCT` with before (1:1) |
| **Line Items** | `ARRAY<STRUCT>` with before | `ARRAY<STRUCT>` with before (1:N) |
| **Records per Order** | Multiple (all versions) | One (current only) |
| **Inserts** | Always append new versions | Upsert based on version |
| **Updates** | Never update, only insert | Update in place if version higher |
| **Before Images** | Inside each array element | `orderBefore` struct + inside details/items |
| **Use Case** | Historical analysis, audit | Current state queries |

---

## Processing Guarantees

```mermaid
flowchart TD
    subgraph G1["Transaction Guarantees"]
        T1[All events in transaction<br/>processed together]
        T2[No partial transactions<br/>emitted]
        T3[Event count validation<br/>from metadata]
        T1 --> T2 --> T3
    end

    subgraph G2["Versioning Guarantees"]
        V1[Entity-level version<br/>tracking]
        V2[Order version independent<br/>of detail/line item versions]
        V3[No version regression<br/>always keep latest]
        V1 --> V2 --> V3
    end

    subgraph G3["ACID Guarantees"]
        A1[Atomic writes<br/>Delta Lake]
        A2[Consistent reads<br/>Snapshot isolation]
        A3[Durable state<br/>RocksDB + Checkpoints]
        A1 --> A2 --> A3
    end

    G1 --> G2 --> G3

    classDef guarantee fill:#9f9,stroke:#333,stroke-width:2px
    class G1,G2,G3 guarantee
```
