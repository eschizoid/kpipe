package io.github.eschizoid.kpipe.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.format.json.JsonFormat;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

/// End-to-end coverage of the [TestStream] driver over a real `KPipeConsumer` + `MockConsumer`,
/// with no Docker. The first test is the PLAN's target-ergonomics sketch verbatim; the rest cover
/// ordering, filter, error (operator throw / malformed payload / null value), metrics, lifecycle,
/// and non-sequential modes.
class TestStreamTest {

  private static final UnaryOperator<Map<String, Object>> ADD_TIMESTAMP = record -> {
    final var copy = new HashMap<>(record);
    copy.put("ts", 1234L);
    return copy;
  };

  private static final Predicate<Map<String, Object>> ACTIVE = record -> Boolean.TRUE.equals(record.get("active"));

  private static Map<String, Object> record(final String id, final boolean active) {
    // fastjson2 deserializes JSON booleans/strings back to Boolean/String, so round-tripping
    // through the format preserves equality for these value shapes.
    final var m = new HashMap<String, Object>();
    m.put("id", id);
    m.put("active", active);
    return m;
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // The PLAN "target ergonomics" sketch, verbatim
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void planTargetErgonomicsVerbatim() {
    final var record1 = record("a", true);
    final var record2 = record("b", false);
    final var addTimestamp = ADD_TIMESTAMP;
    final var active = ACTIVE;

    final var captured = new CapturingSink<Map<String, Object>>();
    final var driver = TestStream.<Map<String, Object>>builder(JsonFormat.INSTANCE)
      .pipe(addTimestamp)
      .filter(active)
      .toCustom(captured)
      .build();
    driver.send(record1);
    driver.send(record2);
    driver.flush();
    assertEquals(List.of(expectedWithTimestamp("a", true)), captured.captured());

    driver.close();
  }

  private static Map<String, Object> expectedWithTimestamp(final String id, final boolean active) {
    final var m = record(id, active);
    m.put("ts", 1234L);
    return m;
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Ordering + pass-through
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void sequentialModePreservesSendOrder() {
    final var captured = new CapturingSink<byte[]>();
    try (final var driver = TestStream.builder(MessageFormat.bytes()).toCustom(captured).build()) {
      for (int i = 0; i < 25; i++) {
        driver.send(("m" + i).getBytes(StandardCharsets.UTF_8));
      }
      driver.flush();

      final var seen = captured
        .captured()
        .stream()
        .map(b -> new String(b, StandardCharsets.UTF_8))
        .toList();
      assertEquals(25, seen.size());
      for (int i = 0; i < 25; i++) {
        assertEquals("m" + i, seen.get(i), "sequential capture order must match send order");
      }
    }
  }

  @Test
  void operatorsChainInDeclarationOrder() {
    final var captured = new CapturingSink<byte[]>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .pipe(b -> (new String(b, StandardCharsets.UTF_8) + "-first").getBytes(StandardCharsets.UTF_8))
        .pipe(b -> (new String(b, StandardCharsets.UTF_8) + "-second").getBytes(StandardCharsets.UTF_8))
        .toCustom(captured)
        .build()
    ) {
      driver.send("x".getBytes(StandardCharsets.UTF_8)).flush();

      assertEquals(1, captured.count());
      assertEquals("x-first-second", new String(captured.captured().getFirst(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void peekObservesWithoutMutating() {
    final var peeked = new CopyOnWriteArrayList<String>();
    final var captured = new CapturingSink<byte[]>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .peek(b -> peeked.add(new String(b, StandardCharsets.UTF_8)))
        .toCustom(captured)
        .build()
    ) {
      driver.send("hello".getBytes(StandardCharsets.UTF_8)).flush();

      assertEquals(List.of("hello"), peeked);
      assertEquals("hello", new String(captured.captured().getFirst(), StandardCharsets.UTF_8));
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Filter path — dropped records are processed, not errors, never reach the sink
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void filteredRecordsAreCountedProcessedNotErrored() {
    final var captured = new CapturingSink<Map<String, Object>>();
    try (
      final var driver = TestStream.<Map<String, Object>>builder(JsonFormat.INSTANCE)
        .filter(ACTIVE)
        .toCustom(captured)
        .build()
    ) {
      driver.send(record("keep-1", true));
      driver.send(record("drop-1", false));
      driver.send(record("drop-2", false));
      driver.send(record("keep-2", true));
      driver.flush();

      assertEquals(
        List.of("keep-1", "keep-2"),
        captured
          .captured()
          .stream()
          .map(m -> m.get("id"))
          .toList(),
        "only records passing the filter reach the sink"
      );
      assertEquals(4L, driver.metrics().get("messagesProcessed"), "filtered records still count as processed");
      assertEquals(0L, driver.metrics().get("processingErrors"), "intentional filtering is not an error");
      assertEquals(List.of(), driver.errors(), "intentional filtering must not report ProcessingErrors");
    }
  }

  @Test
  void pipeReturningNullFiltersTheRecord() {
    final var captured = new CapturingSink<byte[]>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .pipe(b -> b.length > 2 ? b : null)
        .toCustom(captured)
        .build()
    ) {
      driver.send("long-enough".getBytes(StandardCharsets.UTF_8));
      driver.send("ab".getBytes(StandardCharsets.UTF_8));
      driver.flush();

      assertEquals(1, captured.count());
      assertEquals(2L, driver.metrics().get("messagesProcessed"));
      assertEquals(0L, driver.metrics().get("processingErrors"));
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Error paths — operator throw, malformed payload, null value
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void throwingOperatorReportsErrorAndSkipsSink() {
    final var captured = new CapturingSink<Map<String, Object>>();
    try (
      final var driver = TestStream.<Map<String, Object>>builder(JsonFormat.INSTANCE)
        .pipe(m -> {
          if ("boom".equals(m.get("id"))) throw new IllegalArgumentException("deliberate failure");
          return m;
        })
        .toCustom(captured)
        .build()
    ) {
      driver.send("k1", record("ok", true));
      driver.send("k2", record("boom", true));
      driver.send("k3", record("also-ok", true));
      driver.flush();

      assertEquals(
        List.of("ok", "also-ok"),
        captured
          .captured()
          .stream()
          .map(m -> m.get("id"))
          .toList(),
        "the failing record must not reach the sink, later records still flow"
      );
      assertEquals(2L, driver.metrics().get("messagesProcessed"));
      assertEquals(1L, driver.metrics().get("processingErrors"));

      assertEquals(1, driver.errors().size());
      final var error = driver.errors().getFirst();
      assertEquals("k2", error.record().key(), "the error carries the original Kafka record");
      assertInstanceOf(IllegalArgumentException.class, error.exception());
      assertEquals("deliberate failure", error.exception().getMessage());
    }
  }

  @Test
  void malformedPayloadReportsDeserializationError() {
    final var captured = new CapturingSink<Map<String, Object>>();
    try (final var driver = TestStream.<Map<String, Object>>builder(JsonFormat.INSTANCE).toCustom(captured).build()) {
      driver.sendRaw("this is not json{{{".getBytes(StandardCharsets.UTF_8));
      driver.send(record("valid", true));
      driver.flush();

      assertEquals(
        List.of("valid"),
        captured
          .captured()
          .stream()
          .map(m -> m.get("id"))
          .toList(),
        "the malformed record must not poison subsequent valid records"
      );
      assertEquals(1L, driver.metrics().get("processingErrors"));
      assertEquals(1, driver.errors().size());
      assertNotNull(driver.errors().getFirst().exception());
    }
  }

  @Test
  void nullValueRecordReportsError() {
    final var captured = new CapturingSink<byte[]>();
    try (final var driver = TestStream.builder(MessageFormat.bytes()).toCustom(captured).build()) {
      driver.sendRaw((byte[]) null);
      driver.flush();

      assertEquals(0, captured.count());
      assertEquals(1L, driver.metrics().get("processingErrors"));
      assertEquals(1, driver.errors().size());
    }
  }

  @Test
  void customErrorHandlerRunsInAdditionToErrorRecording() {
    final var observed = new CopyOnWriteArrayList<String>();
    final var captured = new CapturingSink<byte[]>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .pipe(b -> {
          throw new IllegalStateException("always fails");
        })
        .withErrorHandler(error -> observed.add(error.exception().getMessage()))
        .toCustom(captured)
        .build()
    ) {
      driver.send("v".getBytes(StandardCharsets.UTF_8)).flush();

      assertEquals(List.of("always fails"), observed, "the user handler must observe the failure");
      assertEquals(1, driver.errors().size(), "the built-in recording must still happen");
    }
  }

  @Test
  void throwingUserErrorHandlerDoesNotBreakErrorRecording() {
    final var captured = new CapturingSink<byte[]>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .pipe(b -> {
          throw new IllegalStateException("fails");
        })
        .withErrorHandler(error -> {
          throw new RuntimeException("handler misbehaves");
        })
        .toCustom(captured)
        .build()
    ) {
      driver.send("v1".getBytes(StandardCharsets.UTF_8));
      driver.send("v2".getBytes(StandardCharsets.UTF_8));
      driver.flush();

      assertEquals(2, driver.errors().size(), "recording happens before the user handler, so both errors land");
      assertEquals(2L, driver.metrics().get("processingErrors"));
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Modes beyond SEQUENTIAL — same records, order-insensitive assertions
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void parallelModeDeliversEveryRecord() {
    final var captured = new CapturingSink<byte[]>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .withProcessingMode(ProcessingMode.PARALLEL)
        .toCustom(captured)
        .build()
    ) {
      final var expected = new HashSet<String>();
      for (int i = 0; i < 40; i++) {
        expected.add("p" + i);
        driver.send(("p" + i).getBytes(StandardCharsets.UTF_8));
      }
      driver.flush();

      final Set<String> seen = new HashSet<>();
      for (final var b : captured.captured()) seen.add(new String(b, StandardCharsets.UTF_8));
      assertEquals(expected, seen, "parallel mode must deliver every record exactly once");
    }
  }

  @Test
  void keyOrderedModeDeliversEveryRecord() {
    final var captured = new CapturingSink<byte[]>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .withProcessingMode(ProcessingMode.KEY_ORDERED)
        .toCustom(captured)
        .build()
    ) {
      final var expected = new HashSet<String>();
      for (int i = 0; i < 20; i++) {
        expected.add("k" + i);
        driver.send("key-" + (i % 3), ("k" + i).getBytes(StandardCharsets.UTF_8));
      }
      driver.flush();

      final Set<String> seen = new HashSet<>();
      for (final var b : captured.captured()) seen.add(new String(b, StandardCharsets.UTF_8));
      assertEquals(expected, seen);
    }
  }

  // ────────────────────────────────────────────────────────────────────────────────
  // Lifecycle + builder validation
  // ────────────────────────────────────────────────────────────────────────────────

  @Test
  void flushWithNothingSentReturnsImmediately() {
    try (final var driver = TestStream.builder(MessageFormat.bytes()).toCustom(new CapturingSink<>()).build()) {
      driver.flush();
    }
  }

  @Test
  void sinkIsOptionalProcessOnlyStreamsWork() {
    final var peeked = new CopyOnWriteArrayList<String>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .peek(b -> peeked.add(new String(b, StandardCharsets.UTF_8)))
        .build()
    ) {
      driver.send("no-sink".getBytes(StandardCharsets.UTF_8)).flush();
      assertEquals(List.of("no-sink"), peeked);
    }
  }

  @Test
  void closeIsIdempotentAndRejectsFurtherSends() {
    final var driver = TestStream.builder(MessageFormat.bytes()).toCustom(new CapturingSink<>()).build();
    driver.send("v".getBytes(StandardCharsets.UTF_8)).flush();
    driver.close();
    driver.close();

    assertThrows(IllegalStateException.class, () -> driver.send("late".getBytes(StandardCharsets.UTF_8)));
    assertThrows(IllegalStateException.class, driver::flush);
  }

  @Test
  void builderRejectsInvalidConfig() {
    assertThrows(NullPointerException.class, () -> TestStream.builder(null));
    assertThrows(NullPointerException.class, () -> TestStream.builder(MessageFormat.bytes()).pipe(null));
    assertThrows(NullPointerException.class, () -> TestStream.builder(MessageFormat.bytes()).filter(null));
    assertThrows(NullPointerException.class, () -> TestStream.builder(MessageFormat.bytes()).toCustom(null));
    assertThrows(IllegalArgumentException.class, () -> TestStream.builder(MessageFormat.bytes()).withTopic(" "));
    assertThrows(IllegalArgumentException.class, () ->
      TestStream.builder(MessageFormat.bytes()).toBatch(batch -> null, 0)
    );
    assertThrows(IllegalArgumentException.class, () ->
      TestStream.builder(MessageFormat.bytes())
        .toCustom(new CapturingSink<>())
        .toBatch(batch -> null, 3)
        .build()
    );
  }

  @Test
  void customTopicFlowsThroughToProcessingErrors() {
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .withTopic("orders")
        .pipe(b -> {
          throw new IllegalStateException("fail");
        })
        .build()
    ) {
      driver.send("v".getBytes(StandardCharsets.UTF_8)).flush();

      assertEquals("orders", driver.errors().getFirst().record().topic());
    }
  }

  @Test
  void metricsExposeReceivedAndInFlightKeys() {
    try (final var driver = TestStream.builder(MessageFormat.bytes()).toCustom(new CapturingSink<>()).build()) {
      driver.send("v".getBytes(StandardCharsets.UTF_8)).flush();

      final var metrics = driver.metrics();
      assertEquals(1L, metrics.get("messagesReceived"));
      assertEquals(1L, metrics.get("messagesProcessed"));
      assertEquals(0L, metrics.get("inFlight"));
      assertTrue(metrics.containsKey("processingErrors"));
    }
  }
}
