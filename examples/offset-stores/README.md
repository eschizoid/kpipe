# Offset Stores Example

Keeps a consumer group's committed offsets in a database instead of Kafka, through the `OffsetStore` interface.

## What it shows

| Class                 | What it is                                                                                                                                                                    |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PostgresOffsetStore` | An `OffsetStore` over a Postgres table keyed by group, topic and partition. A commit writes every partition in one transaction. The table shape is in the class Javadoc.      |
| `DynamoDbOffsetStore` | An `OffsetStore` over a DynamoDB table keyed by `pk` (group, topic and partition), holding the next offset in `next_offset`. Use a global table to fail over between regions. |
| `GlobalTableApp`      | A consumer that passes a `DynamoDbOffsetStore` to `Stream.withOffsetStore` and keeps its offsets in this region's replica of the table.                                       |

`GlobalTableApp` reads its settings from the environment: `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`),
`KAFKA_CONSUMER_GROUP` and `KAFKA_TOPIC` (both `payments`), `OFFSET_TABLE` (`kpipe-offsets`) and `AWS_REGION`
(`us-east-1`). AWS credentials come from the SDK's default provider chain. Run the same app in each region, against that
region's Kafka cluster.

## How a store is called

The default `KafkaOffsetManager` still decides what is safe to commit. The store only ever receives the commit frontier:
for each partition, the lowest offset that has not finished processing.

- **Load on assignment.** The consumer calls `load` with the newly assigned partitions and seeks each one the store
  returns an offset for. A partition left out starts from the group's Kafka offset or from `auto.offset.reset`, so set
  `auto.offset.reset=earliest`, as `GlobalTableApp` does. A `load` that throws is retried with a growing wait, and the
  consumer does not poll while it retries. A result that cannot be applied, a null map or a null or negative offset, is
  logged at ERROR and retried in the same way. An offset for a partition that was not requested is ignored with a
  WARNING.
- **Commit on interval, revoke and close.** `commit` gets the frontier on each commit interval, on the manager's commit
  thread, and on revoke and at close, on the consumer thread. A failed periodic commit is carried by the next one. A
  failed commit on revoke or at close leaves the stored offset where the previous commit put it. Each commit is counted
  in `MetricsSnapshot.commits()`.
- **A loss writes nothing.** A lost partition may already belong to another consumer, so the manager forgets it without
  calling `commit`.
- **No fencing.** The store does not know which consumer owns a partition. A consumer that was evicted but has not
  noticed yet can still write an older frontier, which moves the offset backwards: more reprocessing, never a skip.

## Running the tests

The tests need Docker. Testcontainers starts Postgres, DynamoDB Local and Kafka, and each test class is skipped when
Docker is not available.

```bash
./gradlew :examples:offset-stores:test
```

- `OffsetStoreContract` holds the checks both stores must pass; `PostgresOffsetStoreTest` and `DynamoDbOffsetStoreTest`
  run it against each store.
- `GlobalTableFailoverIntegrationTest` runs one group in two regions, each with its own Kafka cluster and DynamoDB
  table. DynamoDB Local has no global tables, so a background task copies the east table to the west one. The east
  consumer fails, and the test checks that the west consumer resumes from the offsets that replicated, reprocessing what
  did not replicate and skipping nothing.
