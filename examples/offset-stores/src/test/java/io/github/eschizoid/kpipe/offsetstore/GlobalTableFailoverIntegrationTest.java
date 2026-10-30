package io.github.eschizoid.kpipe.offsetstore;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.Handle;
import io.github.eschizoid.kpipe.KPipe;
import io.github.eschizoid.kpipe.consumer.OffsetStore;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.consumer.config.KafkaConsumerConfig;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/// One consumer group in two regions, each with its own Kafka cluster and its own replica of the
/// offset table. East processes until it fails; west takes over from the offsets that replicated.
///
/// Both clusters hold the same records at the same offsets, as a replicated topic that preserves
/// offsets does. DynamoDB Local has no global tables, so a background task copies east's table to
/// west's, and stopping it stands in for the writes still in flight when east fails.
@Testcontainers(disabledWithoutDocker = true)
class GlobalTableFailoverIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");
  private static final String TABLE = DynamoDbLocal.TABLE;
  private static final String GROUP = "payments";
  private static final String TOPIC = "payments";
  private static final int PARTITIONS = 3;
  private static final int RECORDS = 200;
  /// East finishes every record below this, and the table replicates its frontier to west.
  private static final int REPLICATED = 60;
  /// East then finishes every record below this, but the frontier it writes never replicates.
  private static final int UNREPLICATED = 120;

  @Container
  private static final KafkaContainer eastKafka = kafka();

  @Container
  private static final KafkaContainer westKafka = kafka();

  @Container
  private static final GenericContainer<?> eastDynamo = DynamoDbLocal.container();

  @Container
  private static final GenericContainer<?> westDynamo = DynamoDbLocal.container();

  private DynamoDbClient eastTable;
  private DynamoDbClient westTable;

  @BeforeEach
  void setUp() throws Exception {
    eastTable = DynamoDbLocal.client(eastDynamo);
    westTable = DynamoDbLocal.client(westDynamo);
    DynamoDbLocal.createTable(eastTable);
    DynamoDbLocal.createTable(westTable);
    final var eastOffsets = produce(eastKafka);
    final var westOffsets = produce(westKafka);
    assertEquals(eastOffsets, westOffsets, "a replicated topic holds each record at the same offset");
  }

  @AfterEach
  void tearDown() {
    eastTable.close();
    westTable.close();
  }

  /// West resumes at the last frontier that replicated. It processes again the records east
  /// finished after that frontier, and every record east never finished; none is skipped.
  @Test
  void westResumesFromTheReplicatedOffsetWhenEastFails() throws Exception {
    final var replication = new Replication(eastTable, westTable);
    final var east = new RegionOutage(new DynamoDbOffsetStore(eastTable, TABLE, GROUP));
    final var eastSeen = ConcurrentHashMap.<String>newKeySet();
    final var releaseUnreplicated = new CountDownLatch(1);
    final var eastHandle = consumer(eastKafka, east, "earliest", eastSeen, value -> {
      final var index = index(value);
      if (index >= UNREPLICATED) new CountDownLatch(1).await();
      if (index >= REPLICATED) releaseUnreplicated.await();
    });

    awaitTrue(() -> eastSeen.size() == PARTITIONS * REPLICATED, "east finishes the replicated records");
    awaitTrue(() -> frontiers(eastTable).equals(uniform(REPLICATED)), "east stores the frontier");
    awaitTrue(() -> frontiers(westTable).equals(uniform(REPLICATED)), "the frontier replicates to west");
    replication.close();

    releaseUnreplicated.countDown();
    awaitTrue(() -> eastSeen.size() == PARTITIONS * UNREPLICATED, "east finishes the unreplicated records");
    awaitTrue(() -> frontiers(eastTable).equals(uniform(UNREPLICATED)), "east stores the newer frontier");
    east.fail();
    eastHandle.close();

    assertEquals(uniform(REPLICATED), frontiers(westTable), "west holds the last frontier that replicated");

    final var westSeen = ConcurrentHashMap.<String>newKeySet();
    try (
      final var _ = consumer(westKafka, new DynamoDbOffsetStore(westTable, TABLE, GROUP), "latest", westSeen, _ -> {})
    ) {
      awaitTrue(
        () -> westSeen.size() == PARTITIONS * (RECORDS - REPLICATED),
        "west reaches the last records; under auto.offset.reset=latest it processes none without the seek"
      );
    }

    assertEquals(
      values(REPLICATED, RECORDS),
      westSeen,
      "west processes exactly the records from the replicated frontier on"
    );
    final var everyRecord = new HashSet<>(eastSeen);
    everyRecord.addAll(westSeen);
    assertEquals(values(0, RECORDS), everyRecord, "between the two regions, no record is skipped");
    final var processedTwice = new HashSet<>(eastSeen);
    processedTwice.retainAll(westSeen);
    assertEquals(
      values(REPLICATED, UNREPLICATED),
      processedTwice,
      "only the records behind the unreplicated frontier are processed twice"
    );
  }

  /// A sink step that may block; throwing [InterruptedException] ends the record when the consumer
  /// closes.
  @FunctionalInterface
  private interface Step {
    void run(String value) throws InterruptedException;
  }

  private static Handle consumer(
    final KafkaContainer kafka,
    final OffsetStore store,
    final String offsetReset,
    final Set<String> seen,
    final Step step
  ) {
    final var props = KafkaConsumerConfig.createConsumerConfig(kafka.getBootstrapServers(), GROUP);
    props.put("auto.offset.reset", offsetReset);
    props.put("max.poll.records", "20");
    return KPipe.bytes(TOPIC, props)
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withOffsetStore(store)
      .withCommitInterval(Duration.ofMillis(100))
      .withWaitForMessagesTimeout(Duration.ZERO)
      .toCustom(payload -> {
        final var value = new String(payload, UTF_8);
        try {
          step.run(value);
          TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(1, 5));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
        seen.add(value);
      })
      .start();
  }

  /// Fails every call once [#fail] is called, as a store in a region that went down does.
  private static final class RegionOutage implements OffsetStore {

    private final OffsetStore delegate;
    private volatile boolean down;

    RegionOutage(final OffsetStore delegate) {
      this.delegate = delegate;
    }

    void fail() {
      down = true;
    }

    @Override
    public Map<TopicPartition, Long> load(final Collection<TopicPartition> partitions) {
      if (down) throw new IllegalStateException("region down");
      return delegate.load(partitions);
    }

    @Override
    public void commit(final Map<TopicPartition, Long> offsets) {
      if (down) throw new IllegalStateException("region down");
      delegate.commit(offsets);
    }
  }

  /// Copies every item from one table to another every 50 ms. Once closed, later writes to the
  /// source never reach the target.
  private static final class Replication implements AutoCloseable {

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    Replication(final DynamoDbClient source, final DynamoDbClient target) {
      executor.scheduleWithFixedDelay(
        () -> {
          // A thrown exception would cancel every later run; a failed copy is retried on the
          // next.
          try {
            source
              .scan(request -> request.tableName(TABLE).consistentRead(true))
              .items()
              .forEach(item -> target.putItem(request -> request.tableName(TABLE).item(item)));
          } catch (final RuntimeException e) {
            System.getLogger(Replication.class.getName()).log(System.Logger.Level.WARNING, "copy failed", e);
          }
        },
        0,
        50,
        TimeUnit.MILLISECONDS
      );
    }

    @Override
    public void close() throws InterruptedException {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "replication stops");
    }
  }

  private static KafkaContainer kafka() {
    return new KafkaContainer(DockerImageName.parse("apache/kafka:%s".formatted(KAFKA_VERSION)));
  }

  /// Creates the topic and writes `p<partition>-v<index>` to every partition in order, returning
  /// the offset each record landed at.
  private static Map<String, Long> produce(final KafkaContainer kafka) throws Exception {
    try (final var admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get(30, TimeUnit.SECONDS);
    }
    final var offsets = new TreeMap<String, Long>();
    try (
      final var producer = new KafkaProducer<>(
        Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()),
        new ByteArraySerializer(),
        new ByteArraySerializer()
      )
    ) {
      for (int partition = 0; partition < PARTITIONS; partition++) {
        for (int i = 0; i < RECORDS; i++) {
          final var value = "p%d-v%d".formatted(partition, i);
          final var metadata = producer
            .send(new ProducerRecord<>(TOPIC, partition, null, value.getBytes(UTF_8)))
            .get(30, TimeUnit.SECONDS);
          offsets.put(value, metadata.offset());
        }
      }
    }
    return offsets;
  }

  private static Map<TopicPartition, Long> frontiers(final DynamoDbClient table) {
    final var partitions = IntStream.range(0, PARTITIONS)
      .mapToObj(p -> new TopicPartition(TOPIC, p))
      .toList();
    return new DynamoDbOffsetStore(table, TABLE, GROUP).load(partitions);
  }

  private static Map<TopicPartition, Long> uniform(final long offset) {
    return IntStream.range(0, PARTITIONS)
      .boxed()
      .collect(Collectors.toMap(p -> new TopicPartition(TOPIC, p), _ -> offset));
  }

  private static int index(final String value) {
    return Integer.parseInt(value.substring(value.indexOf("-v") + 2));
  }

  private static Set<String> values(final int fromInclusive, final int toExclusive) {
    return IntStream.range(0, PARTITIONS)
      .boxed()
      .flatMap(p -> IntStream.range(fromInclusive, toExclusive).mapToObj(i -> "p%d-v%d".formatted(p, i)))
      .collect(Collectors.toSet());
  }

  private static void awaitTrue(final BooleanSupplier condition, final String description) throws InterruptedException {
    final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) fail("timed out waiting until " + description);
      TimeUnit.MILLISECONDS.sleep(20);
    }
  }
}
