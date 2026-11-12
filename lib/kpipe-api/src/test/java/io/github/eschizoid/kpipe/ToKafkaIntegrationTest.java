package io.github.eschizoid.kpipe;

import static io.github.eschizoid.kpipe.record.RecordContext.current;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.record.RecordContext;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// `toKafka` against a real broker: each output record keeps its input record's key and headers,
/// operators read the record through [RecordContext#current()], and the record is bound per
/// attempt and only on the processing thread.
@Testcontainers(disabledWithoutDocker = true)
class ToKafkaIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");
  private static final int RECORDS = 30;
  private static final int KEYS = 5;

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("apache/kafka:%s".formatted(KAFKA_VERSION))
  ).withStartupAttempts(3);

  /// In every mode, a header filter keeps half the records, a failing first attempt retries a
  /// third of them, and every output record carries its input key and headers.
  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void outputRecordsKeepTheirInputKeyAndHeaders(final ProcessingMode mode) throws Exception {
    final var in = "to-kafka-in-" + UUID.randomUUID();
    final var out = "to-kafka-out-" + UUID.randomUUID();
    createTopics(in, out);
    produce(in, RECORDS);

    final var attempts = new ConcurrentHashMap<Long, AtomicInteger>();
    try (
      final var _ = KPipe.json(in, consumerProps())
        .withProcessingMode(mode)
        .withRetry(2, Duration.ofMillis(10))
        .filter(_ -> current().headerString("tenant").orElse("").equals("acme"))
        .pipe(order -> {
          final var id = id(order);
          final var n = attempts.computeIfAbsent(id, _ -> new AtomicInteger()).incrementAndGet();
          if (id % 3 == 0 && n == 1) throw new IllegalStateException("first attempt fails");
          order.put("sourceOffset", current().offset());
          return order;
        })
        .toKafka(out)
        .start()
    ) {
      final var received = readAll(out, RECORDS / 2);
      assertEquals(RECORDS / 2, received.size());
      for (final var record : received) {
        final var value = new String(record.value(), UTF_8);
        final var id = Long.parseLong(value.replaceAll(".*\"id\":(\\d+).*", "$1"));
        assertEquals("k" + (id % KEYS), new String(record.key(), UTF_8), "key for id " + id);
        assertEquals("acme", new String(record.headers().lastHeader("tenant").value(), UTF_8));
        assertEquals("" + id, new String(record.headers().lastHeader("source-id").value(), UTF_8));
        assertTrue(value.contains("\"sourceOffset\":"), value);
        if (id % 3 == 0) assertEquals(2, attempts.get(id).get(), "attempts for id " + id);
      }
    }
  }

  /// `KEY_ORDERED` processes each key in input order, and `toKafka` keeps the key, so a key's
  /// records land on one output partition in input order.
  @Test
  void keyOrderedOutputKeepsPerKeyOrderOnOnePartition() throws Exception {
    final var in = "to-kafka-ordered-in-" + UUID.randomUUID();
    final var out = "to-kafka-ordered-out-" + UUID.randomUUID();
    createTopics(in, out);
    produce(in, RECORDS);

    try (
      final var _ = KPipe.json(in, consumerProps()).withProcessingMode(ProcessingMode.KEY_ORDERED).toKafka(out).start()
    ) {
      final var received = readAll(out, RECORDS);
      assertEquals(RECORDS, received.size());
      final var partitionByKey = new HashMap<String, Integer>();
      final var idsByKey = new HashMap<String, List<Long>>();
      for (final var record : received) {
        final var key = new String(record.key(), UTF_8);
        final var previous = partitionByKey.putIfAbsent(key, record.partition());
        if (previous != null) assertEquals(previous, record.partition(), "one partition for key " + key);
        idsByKey.computeIfAbsent(key, _ -> new ArrayList<>()).add(idOf(record));
      }
      for (final var entry : idsByKey.entrySet()) {
        final var ids = entry.getValue();
        assertEquals(ids.stream().sorted().toList(), ids, "input order for key " + entry.getKey());
      }
    }
  }

  /// With a dead-letter topic as well, a failing record goes to the dead-letter topic with its key
  /// and the rest to the output topic.
  @Test
  void aFailingRecordGoesToTheDeadLetterTopicAndTheRestToTheOutput() throws Exception {
    final var in = "to-kafka-dlq-in-" + UUID.randomUUID();
    final var out = "to-kafka-dlq-out-" + UUID.randomUUID();
    final var dlq = "to-kafka-dlq-" + UUID.randomUUID();
    createTopics(in, out, dlq);
    produce(in, 10);

    try (
      final var _ = KPipe.json(in, consumerProps())
        .withDeadLetterTopic(dlq)
        .pipe(order -> {
          if (id(order) == 4) throw new IllegalStateException("poison");
          return order;
        })
        .toKafka(out)
        .start()
    ) {
      assertEquals(9, readAll(out, 9).size());
      final var dead = readAll(dlq, 1);
      assertEquals(1, dead.size());
      assertEquals("k4", new String(dead.getFirst().key(), UTF_8));
    }
  }

  /// Every `KPipe.multi` route can end in `toKafka`, each record keeping its key.
  @Test
  void aMultiRouteCanProduceToKafka() throws Exception {
    final var a = "to-kafka-multi-a-" + UUID.randomUUID();
    final var b = "to-kafka-multi-b-" + UUID.randomUUID();
    final var out = "to-kafka-multi-out-" + UUID.randomUUID();
    createTopics(a, b, out);
    produce(a, 3);
    produce(b, 4);

    try (
      final var _ = KPipe.multi(consumerProps())
        .json(a, s -> s.toKafka(out))
        .json(b, s -> s.toKafka(out))
        .start()
    ) {
      final var received = readAll(out, 7);
      assertEquals(7, received.size());
      for (final var record : received) assertEquals("k" + (idOf(record) % KEYS), new String(record.key(), UTF_8));
    }
  }

  /// A batch sink covers many records, so no record is bound when a full buffer flushes on the
  /// thread of the record that filled it.
  @Test
  void aSizeTriggeredFlushHasNoCurrentRecord() throws Exception {
    final var flush = batchFlush(new BatchPolicy(5, Duration.ofMinutes(1)), 5);
    assertEquals("unbound", flush.binding());
    assertNotEquals("kpipe-scheduler", flush.thread(), "flushed by size");
  }

  /// Nor when the age tick flushes on the scheduler thread.
  @Test
  void anAgeTriggeredFlushHasNoCurrentRecord() throws Exception {
    final var flush = batchFlush(new BatchPolicy(1_000, Duration.ofMillis(200)), 2);
    assertEquals("unbound", flush.binding());
    assertEquals("kpipe-scheduler", flush.thread(), "flushed by age");
  }

  private record Flush(String binding, String thread) {}

  private static Flush batchFlush(final BatchPolicy policy, final int records) throws Exception {
    final var in = "to-kafka-batch-" + UUID.randomUUID();
    createTopics(in);
    final var flushes = new CopyOnWriteArrayList<Flush>();
    try (
      final var _ = KPipe.json(in, consumerProps())
        .withProcessingMode(ProcessingMode.SEQUENTIAL)
        .toBatch(
          values -> {
            flushes.add(new Flush(RecordContext.isBound() ? "bound" : "unbound", Thread.currentThread().getName()));
            return BatchResult.allSucceeded(values.size());
          },
          policy
        )
        .start()
    ) {
      produce(in, records);
      final var deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (flushes.isEmpty() && System.nanoTime() < deadline) Thread.sleep(50);
      // Read the flush before close, whose drain would flush the buffer on this thread.
      assertFalse(flushes.isEmpty(), "no flush before close");
      return flushes.getFirst();
    }
  }

  /// An uncaptured `current()` on another thread throws the clear message; a captured context
  /// works there.
  @Test
  void aCapturedContextCrossesThreads() throws Exception {
    final var in = "to-kafka-handoff-" + UUID.randomUUID();
    createTopics(in);
    final var captured = new AtomicReference<String>();
    final var unbound = new AtomicReference<Throwable>();
    final var exec = Executors.newSingleThreadExecutor();
    try (
      final var _ = KPipe.json(in, consumerProps())
        .toCustom(_ -> {
          final var ctx = current();
          try {
            captured.set(exec.submit(() -> ctx.topic() + "@" + ctx.offset()).get());
            exec.submit(RecordContext::current).get();
          } catch (final ExecutionException e) {
            unbound.set(e.getCause());
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        })
        .start()
    ) {
      produce(in, 1);
      final var deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (unbound.get() == null && System.nanoTime() < deadline) Thread.sleep(50);
    } finally {
      exec.shutdown();
    }
    assertTrue(captured.get().matches(".*@0"), captured.get());
    assertInstanceOf(IllegalStateException.class, unbound.get());
    assertTrue(unbound.get().getMessage().startsWith("No record is being processed on this thread"));
  }

  private static long id(final Map<String, Object> order) {
    return ((Number) order.get("id")).longValue();
  }

  private static long idOf(final ConsumerRecord<byte[], byte[]> record) {
    return Long.parseLong(new String(record.value(), UTF_8).replaceAll(".*\"id\":(\\d+).*", "$1"));
  }

  /// Records `{"id":i}` keyed `k(i % KEYS)`, with a `tenant` header alternating `acme` and
  /// `other` and a `source-id` header carrying `i`.
  private static void produce(final String topic, final int count) {
    try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps())) {
      for (int i = 0; i < count; i++) {
        final var record = new ProducerRecord<>(
          topic,
          ("k" + (i % KEYS)).getBytes(UTF_8),
          ("{\"id\":" + i + "}").getBytes(UTF_8)
        );
        record.headers().add("tenant", (i % 2 == 0 ? "acme" : "other").getBytes(UTF_8));
        record.headers().add("source-id", ("" + i).getBytes(UTF_8));
        producer.send(record);
      }
      producer.flush();
    }
  }

  private static List<ConsumerRecord<byte[], byte[]>> readAll(final String topic, final int expected) {
    final var received = new ArrayList<ConsumerRecord<byte[], byte[]>>();
    try (final var consumer = new KafkaConsumer<byte[], byte[]>(consumerProps())) {
      consumer.subscribe(List.of(topic));
      final var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (received.size() < expected && System.nanoTime() < deadline) {
        consumer.poll(Duration.ofMillis(200)).forEach(received::add);
      }
    }
    received.sort((x, y) -> Long.compare(x.offset(), y.offset()));
    return received;
  }

  private static void createTopics(final String... topics) throws Exception {
    try (
      final var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))
    ) {
      admin
        .createTopics(
          List.of(topics)
            .stream()
            .map(t -> new NewTopic(t, 4, (short) 1))
            .toList()
        )
        .all()
        .get();
    }
  }

  private static Properties producerProps() {
    final var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
    return props;
  }

  private static Properties consumerProps() {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "to-kafka-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return props;
  }
}
