# Spec Delta

## Purpose

Defines how a complete source transaction becomes stage 1 output records for a job's model: grouping by root key, one array per entity, typed columns with before images, and the handling of source values that do not fit the model.

## ADDED Requirements

### Requirement: One record per transaction and root key
For each emitted transaction, the job SHALL write one stage 1 record per distinct root key value found in the transaction's output events. Each record SHALL hold the transaction's `xid` and `csn`, a processing timestamp `dwhProcessedTs`, the root key value, and one array per entity holding that entity's events for that key in their buffered order.

#### Scenario: Transaction touching two orders
- **WHEN** a transaction updates order 7 and one of its line items, and inserts order 8 with two line items
- **THEN** two records are written: one for order 7 with one order and one line item, and one for order 8 with one order and two line items

#### Scenario: Entity absent for a key
- **WHEN** a transaction changes only a line item of order 7
- **THEN** order 7's record has an empty orders array, an empty order details array, and one line item

### Requirement: Entity elements are typed and carry before images
Each array element SHALL hold the entity's output columns, each cast from the event's after image to the column's output type. When the entity's before image is on, the element SHALL also hold a `before` struct with the same columns cast from the before image. The struct SHALL be null when the event has no before image.

#### Scenario: Update with a before image
- **WHEN** an update to order 7 changes `ORDER_STATUS` from `NEW` to `SHIPPED`
- **THEN** the element has `orderStatus = SHIPPED` and `before.orderStatus = NEW`

#### Scenario: Insert
- **WHEN** order 8 is inserted
- **THEN** its element has a null `before`

### Requirement: Events without an after image produce no element
An event with no after image, such as a delete, SHALL count toward completion but SHALL NOT produce an array element.

#### Scenario: Delete in the transaction
- **WHEN** a completed transaction holds one line-item update and one line-item delete for order 7
- **THEN** order 7's record has one line-item element

### Requirement: Unmapped and missing source columns
A source column that is not in the entity's model, or that the job excludes, SHALL be left out of the output. A column the job has not excluded SHALL be warned about once per run and table. A model column missing from an event SHALL be null in the output.

#### Scenario: New column not yet in the model
- **WHEN** order events carry column `PRIORITY`, which the model schema does not contain
- **THEN** records are written without it, and one warning names `ORDERS.PRIORITY`

#### Scenario: Column missing from the event
- **WHEN** an order event's after image has no `SHIPPING_ADDRESS_ID`
- **THEN** that element's `shippingAddressId` is null

### Requirement: Values that fail to cast become null and are reported
A non-null source value that cannot be cast to its column's output type SHALL become null in the output. It SHALL NOT drop the event or block the transaction. The job SHALL log, for each micro-batch, how many values failed per table and column.

#### Scenario: Malformed number
- **WHEN** an order event has `TOTAL_AMOUNT = "12,50"` and the column's type is `decimal(20,4)`
- **THEN** the transaction is emitted with `totalAmount` null, and the batch's log reports one cast failure for `ORDERS.TOTAL_AMOUNT`

### Requirement: Source timestamps use the configured format
Source date and timestamp values SHALL be parsed with the timestamp format configured in shared defaults. A job SHALL be able to override that format.

#### Scenario: GoldenGate timestamp string
- **WHEN** `ORDER_TS` is `2026-11-11 10:15:30.123456000` and the column's output type is `timestamp`
- **THEN** `orderTs` is 2026-11-11 10:15:30.123456
