package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.format.json.JsonFormat;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.management.ObjectName;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class ToKafkaBuildTest {

  private static Properties props() {
    final var props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "test-group");
    return props;
  }

  @Test
  void toKafkaTerminatesWithAKafkaTopicSink() {
    final var sink = (DefaultSink<?>) KPipe.json("in", props()).toKafka("out");
    final var terminal = assertInstanceOf(KafkaTopicSink.class, sink.terminalSink());
    assertEquals("out", terminal.topic());
  }

  @Test
  void aBlankOrNullTopicIsRejected() {
    final var stream = KPipe.json("in", props());
    assertThrows(IllegalArgumentException.class, () -> stream.toKafka(" "));
    assertThrows(NullPointerException.class, () -> stream.toKafka(null));
  }

  @Test
  void aSchemaRegistryStreamIsRejectedBecauseItsFormatCannotSerialize() {
    final var stream = KPipe.avro("in", props(), (SchemaResolver) id -> "{}");
    final var e = assertThrows(IllegalStateException.class, () -> stream.toKafka("out"));
    assertTrue(e.getMessage().contains("Schema-Registry-backed format only reads"), e.getMessage());
  }

  @Test
  void theSinkRefusesToSendBeforeItsConsumerStarts() {
    @SuppressWarnings("unchecked")
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("in", props()).toKafka("out");
    final var e = assertThrows(IllegalStateException.class, () -> sink.terminalSink().accept(new HashMap<>()));
    assertTrue(e.getMessage().contains("only once its consumer has started"), e.getMessage());
  }

  /// A consumer that fails to build must not leak the producer `toKafka` opened for it, and its
  /// sinks can be opened again by a later start.
  @Test
  void aConsumerThatFailsToBuildClosesTheProducerAndFreesTheSinks() {
    final var sink = KafkaTopicSink.of("out", JsonFormat.INSTANCE);
    final var opened = KafkaTopicSink.openAll(List.of(sink), props(), null, KPipeConsumer.builder());
    final var failure = new IllegalArgumentException("bad config");

    final var thrown = assertThrows(IllegalArgumentException.class, () ->
      KafkaTopicSink.closingOnFailure(opened, () -> {
        throw failure;
      })
    );

    assertSame(failure, thrown);
    final var e = assertThrows(IllegalStateException.class, () ->
      opened.producer().sendAsync(new ProducerRecord<>("out", new byte[0]))
    );
    assertTrue(e.getMessage().contains("closed"), e.getMessage());
    KafkaTopicSink.openAll(List.of(sink), props(), null, KPipeConsumer.builder()).abandon();
  }

  /// A `toKafka` sink belongs to one consumer: opening it again, as a second `MultiBuilder.start()`
  /// would, throws.
  @Test
  void aSinkThatAlreadyBelongsToAConsumerCannotBeOpenedAgain() {
    final var sink = KafkaTopicSink.of("out", JsonFormat.INSTANCE);
    final var opened = KafkaTopicSink.openAll(List.of(sink), props(), null, KPipeConsumer.builder());
    try {
      final var e = assertThrows(IllegalStateException.class, () ->
        KafkaTopicSink.openAll(List.of(sink), props(), null, KPipeConsumer.builder())
      );
      assertTrue(e.getMessage().contains("already open for another consumer"), e.getMessage());
    } finally {
      opened.abandon();
    }
  }

  /// When the consumer fails to build, `start()` closes the producer `toKafka` opened for it. A
  /// Kafka producer registers an `app-info` MBean under its client id until it closes.
  @Test
  void startClosesTheProducerWhenTheConsumerFailsToBuild() throws Exception {
    final var props = props();
    props.setProperty("client.id", "to-kafka-leak-test");
    props.setProperty("key.deserializer", "no.such.Deserializer");
    final var mbean = new ObjectName("kafka.producer:type=app-info,id=to-kafka-leak-test-producer");

    assertThrows(RuntimeException.class, () -> KPipe.json("in", props).toKafka("out").start());

    assertFalse(ManagementFactory.getPlatformMBeanServer().isRegistered(mbean), "the producer is still open");
  }
}
