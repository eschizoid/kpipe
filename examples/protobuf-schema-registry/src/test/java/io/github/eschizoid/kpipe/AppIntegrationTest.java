package io.github.eschizoid.kpipe;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.OffsetManager;
import io.github.eschizoid.kpipe.consumer.OffsetState;
import io.github.eschizoid.kpipe.consumer.OffsetStatistics;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// Docker-free end-to-end test of the per-record Protobuf + Confluent Schema Registry decode path —
/// the headline feature this example module ships.
///
/// A Confluent-format wire record (1 magic byte `0x00` + 4-byte big-endian schema id +
/// message-index array + a `DynamicMessage` body) is assembled exactly as `KafkaProtobufSerializer`
/// writes it, seeded into a [MockConsumer], and driven through a real, started [KPipeConsumer] whose
/// pipeline is built from `ProtobufFormat.withRegistry(resolver)` — the same registry-mode format
/// `KPipe.protobuf(topic, props, resolver)` wires in `App`. The resolver returns the `.proto` source
/// text for the schema id (what Confluent SR serves), so the REAL
/// `ConfluentProtobufDescriptorCompiler` (discovered from `kpipe-format-protobuf-confluent` via
/// `ServiceLoader`) compiles the writer schema and decodes the payload against it.
///
/// The bytes are hand-assembled rather than generated with `KafkaProtobufSerializer` on purpose: the
/// `kpipe-format-protobuf-confluent` jar is SHADED (it relocates `com.google.common`), and putting it
/// on the same classpath as the un-shaded Confluent serializer stack triggers a guava
/// `Ticker`-signature clash. The envelope layout used here is the exact format that module's own
/// `ProtobufConfluentWireCompatTest` proves wire-compatible against Confluent's real serializer, so
/// the byte shape is not in doubt — this test pins the CONSUMER decode path end to end without Docker.
class AppIntegrationTest {

  private static final String TOPIC = "customer-protobuf-sr";
  private static final int SCHEMA_ID = 7;

  /// The `.proto` source Confluent SR would serve for [#SCHEMA_ID]. The real compiler parses this.
  private static final String CUSTOMER_PROTO =
    """
    syntax = "proto3";
    package com.kpipe.catalog;
    message Customer {
      int64 id = 1;
      string name = 2;
      string email = 3;
      bool active = 4;
    }
    """;

  @Test
  void decodesConfluentProtobufWireBytesEndToEnd() throws Exception {
    // The message a producer would write, matching CUSTOMER_PROTO field-for-field.
    final var customerDescriptor = customerDescriptor();
    final var body = DynamicMessage.newBuilder(customerDescriptor)
      .setField(customerDescriptor.findFieldByName("id"), 42L)
      .setField(customerDescriptor.findFieldByName("name"), "Mariano")
      .setField(customerDescriptor.findFieldByName("email"), "mariano@example.com")
      .setField(customerDescriptor.findFieldByName("active"), true)
      .build();
    final var confluentBytes = confluentEnvelope(SCHEMA_ID, body);

    // Resolver mirrors App's CachedSchemaResolver(ConfluentSchemaResolver(...)), minus the network:
    // id -> .proto source text. The real ConfluentProtobufDescriptorCompiler compiles it.
    final SchemaResolver resolver = schemaId -> {
      if (schemaId != SCHEMA_ID) throw new IllegalStateException("unexpected schema id " + schemaId);
      return CUSTOMER_PROTO;
    };

    final var captured = new CopyOnWriteArrayList<Message>();
    final MessageSink<Message> capturingSink = captured::add;
    final var pipeline = new MessageProcessorRegistry()
      .pipeline(ProtobufFormat.withRegistry(resolver))
      .toSink(capturingSink)
      .build();

    final var offsetManager = new MarkRecordingOffsetManager();
    final var mock = seededWith(confluentBytes);

    final var consumer = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withTopic(TOPIC)
      // SEQUENTIAL so a single seeded record deterministically drains before assertions.
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .withPipeline(pipeline)
      .withConsumer(() -> mock)
      .withOffsetManager(offsetManager)
      .withPollTimeout(Duration.ofMillis(5))
      .build();

    try {
      consumer.start();
      awaitCondition(() -> !captured.isEmpty() && offsetManager.marked.contains(0L), 10_000);

      final var decoded = captured.getFirst();
      final var desc = decoded.getDescriptorForType();
      assertAll(
        () -> assertEquals("Customer", desc.getName(), "decoded the writer message type resolved from the envelope"),
        () -> assertEquals(42L, decoded.getField(desc.findFieldByName("id"))),
        () -> assertEquals("Mariano", decoded.getField(desc.findFieldByName("name"))),
        () -> assertEquals("mariano@example.com", decoded.getField(desc.findFieldByName("email"))),
        () -> assertEquals(true, decoded.getField(desc.findFieldByName("active")))
      );
      assertTrue(offsetManager.marked.contains(0L), "the record's offset must be marked processed (no re-fetch loop)");
    } finally {
      consumer.close();
    }

    assertFalse(consumer.isRunning(), "consumer must reach terminal state after close()");
  }

  /// Builds a Customer [Descriptor] matching [#CUSTOMER_PROTO], used only to encode the body bytes
  /// (the consumer resolves its own descriptor from the schema id via the real compiler).
  private static Descriptor customerDescriptor() throws Exception {
    final var proto = FileDescriptorProto.newBuilder()
      .setName("catalog.proto")
      .setSyntax("proto3")
      .setPackage("com.kpipe.catalog")
      .addMessageType(
        DescriptorProto.newBuilder()
          .setName("Customer")
          .addField(field("id", 1, Type.TYPE_INT64))
          .addField(field("name", 2, Type.TYPE_STRING))
          .addField(field("email", 3, Type.TYPE_STRING))
          .addField(field("active", 4, Type.TYPE_BOOL))
      )
      .build();
    return FileDescriptor.buildFrom(proto, new FileDescriptor[0]).getMessageTypes().get(0);
  }

  private static FieldDescriptorProto field(final String name, final int number, final Type type) {
    return FieldDescriptorProto.newBuilder()
      .setName(name)
      .setNumber(number)
      .setType(type)
      .setLabel(Label.LABEL_OPTIONAL)
      .build();
  }

  /// Assembles the Confluent Protobuf wire envelope: 1-byte magic `0x00`, 4-byte big-endian schema
  /// id, then the single-message shorthand (`0x00`) for the first top-level message, then the
  /// message body. This is exactly what `KafkaProtobufSerializer` emits for a single-message schema.
  private static byte[] confluentEnvelope(final int schemaId, final Message body) {
    final var out = new ByteArrayOutputStream();
    out.write(0x00); // magic
    out.write((schemaId >>> 24) & 0xff);
    out.write((schemaId >>> 16) & 0xff);
    out.write((schemaId >>> 8) & 0xff);
    out.write(schemaId & 0xff);
    out.write(0x00); // message-index shorthand: [0] -> first top-level message
    final var bytes = body.toByteArray();
    out.write(bytes, 0, bytes.length);
    return out.toByteArray();
  }

  private static Properties consumerProps() {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "protobuf-sr-test-group");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("enable.auto.commit", "false");
    return props;
  }

  /// Seeds a single Confluent-envelope record at offset 0. `subscribe` is stubbed to a no-op so the
  /// manual `assign` survives (the consumer subscribes by topic; the mock only honors the assign).
  private static MockConsumer<byte[], byte[]> seededWith(final byte[] value) {
    final var mock = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    mock.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "customer-key".getBytes(UTF_8), value));
    mock.updateEndOffsets(Map.of(tp, 1L));
    return mock;
  }

  private static void awaitCondition(final BooleanSupplier cond, final long timeoutMs) throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeoutMs;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() >= deadline) throw new AssertionError(
        "timed out after " + timeoutMs + "ms waiting for the seeded record to decode and mark"
      );
      Thread.sleep(10);
    }
  }

  /// Minimal offset manager that records which offsets were marked processed, so the test can assert
  /// the commit frontier advances past the seeded record.
  private static final class MarkRecordingOffsetManager implements OffsetManager {

    final Set<Long> marked = ConcurrentHashMap.newKeySet();

    @Override
    public OffsetManager start() {
      return this;
    }

    @Override
    public OffsetManager stop() {
      return this;
    }

    @Override
    public void trackOffset(final ConsumerRecord<byte[], byte[]> record) {}

    @Override
    public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
      marked.add(record.offset());
    }

    @Override
    public void notifyCommitComplete(final String commitId, final boolean success) {}

    @Override
    public OffsetState getState() {
      return OffsetState.CREATED;
    }

    @Override
    public boolean isRunning() {
      return true;
    }

    @Override
    public OffsetStatistics getStatistics() {
      return OffsetStatistics.empty();
    }

    @Override
    public void close() {}
  }
}
