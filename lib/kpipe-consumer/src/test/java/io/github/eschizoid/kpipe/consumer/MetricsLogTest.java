package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricsLogTest {

  private static final String TOPIC = "orders";

  private final List<String> lines = new CopyOnWriteArrayList<>();
  private final Logger logger = Logger.getLogger("io.github.eschizoid.kpipe.metrics");
  private final Handler handler = new Handler() {
    @Override
    public void publish(final LogRecord record) {
      lines.add(record.getMessage());
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  };

  @BeforeEach
  void captureTheMetricsLogger() {
    logger.addHandler(handler);
  }

  @AfterEach
  void releaseTheMetricsLogger() {
    logger.removeHandler(handler);
  }

  private static Properties properties() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "billing");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return p;
  }

  @Test
  void theLineNamesEveryFigureLikeTheOtelInstruments() {
    final var records = new MetricsSnapshot.Records(10, 6, 2, 1, 4, 1, 0, Duration.ofMillis(30));
    final var snapshot = new MetricsSnapshot(
      records,
      Map.of(TOPIC, records),
      3,
      Map.of(TOPIC, 7L),
      new MetricsSnapshot.Backpressure(5, Duration.ofMillis(1500)),
      new MetricsSnapshot.CircuitBreaker(2, Duration.ofMillis(250))
    );

    assertEquals(
      "kpipe metrics [orders group=billing] received=10 processed=6 filtered=2 failed=1 retries=4 " +
        "dlq.sent=1 dlq.failed=0 in_flight=3 lag=7 processing.avg=5.0ms backpressure.pauses=5 " +
        "backpressure.time=1500.0ms circuit_breaker.state=half_open circuit_breaker.trips=2 " +
        "circuit_breaker.open_time=250.0ms",
      MetricsLog.line(List.of(TOPIC), "billing", snapshot, CircuitBreakerState.HALF_OPEN)
    );
    assertTrue(
      MetricsLog.line(List.of("a", "b"), null, MetricsSnapshot.empty(), CircuitBreakerState.CLOSED).startsWith(
        "kpipe metrics [a,b] received=0 "
      )
    );
  }

  /// The final line is written after the shutdown flush, so it counts a record that was still
  /// buffered when `close()` began.
  @Test
  void theFinalLineAtCloseCountsTheShutdownFlush() {
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties())
      .withConsumer(() -> new MockConsumer<>("earliest"))
      .withBatchPipeline(TOPIC, TestPipelines.identity(), BatchSink.ofVoid(_ -> {}), BatchPolicy.ofSize(100))
      .withMetricsLog(Duration.ofHours(1))
      .build();
    consumer.start();
    consumer.processRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k".getBytes(), "v".getBytes()));
    assertEquals(List.of(), lines, "no line before the first interval");

    consumer.close();
    assertEquals(1, lines.size(), () -> "one final line: " + lines);
    assertTrue(lines.getFirst().contains(" processed=1 "), lines.getFirst());
  }

  @Test
  void aConsumerClosedBeforeStartWritesNoLine() {
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties())
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> new MockConsumer<>("earliest"))
      .withMetricsLog(Duration.ofMillis(10))
      .build();
    consumer.close();
    assertEquals(List.of(), lines);
  }

  @Test
  void withoutTheSettingNothingIsLogged() {
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties())
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> new MockConsumer<>("earliest"))
      .build();
    consumer.start();
    consumer.close();
    assertEquals(List.of(), lines);
  }
}
