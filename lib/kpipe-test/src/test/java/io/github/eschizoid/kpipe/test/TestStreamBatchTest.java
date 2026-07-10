package io.github.eschizoid.kpipe.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/// Batch-mode coverage for [TestStream]: size-triggered flushes are observable after `flush()`,
/// the trailing partial batch flushes on `close()` (the production shutdown-drain guarantee), and
/// per-record processing errors inside a batch route surface through `errors()`.
class TestStreamBatchTest {

  private static byte[] bytes(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static List<String> strings(final List<byte[]> batch) {
    return batch
      .stream()
      .map(b -> new String(b, StandardCharsets.UTF_8))
      .toList();
  }

  @Test
  void sizeTriggeredBatchesFlushBeforeClose() {
    final var batches = new CopyOnWriteArrayList<List<String>>();
    final var driver = TestStream.builder(MessageFormat.bytes())
      .toBatch(BatchSink.ofVoid(batch -> batches.add(strings(batch))), 3)
      .build();
    try {
      for (int i = 0; i < 7; i++) driver.send(bytes("b" + i));
      driver.flush();

      // 7 records with size 3: two full batches flushed on the size trigger, one record buffered.
      assertEquals(
        List.of(List.of("b0", "b1", "b2"), List.of("b3", "b4", "b5")),
        batches,
        "full batches must flush on the size trigger, in order, before close"
      );
      assertEquals(6L, driver.metrics().get("messagesProcessed"), "only flushed records count as processed");
      assertEquals(1L, driver.metrics().get("inFlight"), "the trailing record stays buffered until close");
    } finally {
      driver.close();
    }

    // Shutdown flush: the trailing partial batch drains on close.
    assertEquals(3, batches.size(), "close() must flush the trailing partial batch");
    assertEquals(List.of("b6"), batches.getLast());
  }

  @Test
  void closeWithoutSizeTriggerFlushesEverything() {
    final var batches = new CopyOnWriteArrayList<List<String>>();
    final var driver = TestStream.builder(MessageFormat.bytes())
      .toBatch(BatchSink.ofVoid(batch -> batches.add(strings(batch))), 5)
      .build();
    try {
      driver.send(bytes("only-1"));
      driver.send(bytes("only-2"));
      driver.flush();

      assertEquals(List.of(), batches, "below the size trigger nothing flushes before close");
    } finally {
      driver.close();
    }

    assertEquals(List.of(List.of("only-1", "only-2")), batches);
  }

  @Test
  void exactMultipleOfBatchSizeLeavesNothingBuffered() {
    final var batches = new CopyOnWriteArrayList<List<String>>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .toBatch(BatchSink.ofVoid(batch -> batches.add(strings(batch))), 2)
        .build()
    ) {
      for (int i = 0; i < 4; i++) driver.send(bytes("e" + i));
      driver.flush();

      assertEquals(List.of(List.of("e0", "e1"), List.of("e2", "e3")), batches);
      assertEquals(0L, driver.metrics().get("inFlight"), "an exact multiple leaves no buffered records");
      assertEquals(4L, driver.metrics().get("messagesProcessed"));
    }
  }

  @Test
  void operatorsRunBeforeBatchBuffering() {
    final var batches = new CopyOnWriteArrayList<List<String>>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .pipe(b -> (new String(b, StandardCharsets.UTF_8) + "!").getBytes(StandardCharsets.UTF_8))
        .toBatch(BatchSink.ofVoid(batch -> batches.add(strings(batch))), 2)
        .build()
    ) {
      driver.send(bytes("x")).send(bytes("y")).flush();

      assertEquals(List.of(List.of("x!", "y!")), batches, "the batch sink sees post-pipeline values");
    }
  }

  @Test
  void filteredRecordsNeverEnterTheBatchBuffer() {
    final var batches = new CopyOnWriteArrayList<List<String>>();
    try (
      final var driver = TestStream.builder(MessageFormat.bytes())
        .filter(b -> !new String(b, StandardCharsets.UTF_8).startsWith("drop"))
        .toBatch(BatchSink.ofVoid(batch -> batches.add(strings(batch))), 2)
        .build()
    ) {
      driver.send(bytes("keep-1"));
      driver.send(bytes("drop-1"));
      driver.send(bytes("keep-2"));
      driver.flush();

      assertEquals(List.of(List.of("keep-1", "keep-2")), batches, "filtered records are terminal, not buffered");
      assertEquals(3L, driver.metrics().get("messagesProcessed"), "filtered records still count as processed");
      assertEquals(0L, driver.metrics().get("inFlight"));
    }
  }

  @Test
  void throwingBatchSinkReportsEveryRecordInTheBatch() {
    final BatchSink<byte[]> failing = BatchSink.ofVoid(batch -> {
      throw new IllegalStateException("batch sink down");
    });
    try (final var driver = TestStream.builder(MessageFormat.bytes()).toBatch(failing, 2).build()) {
      driver.send(bytes("f1"));
      driver.send(bytes("f2"));
      driver.flush();

      assertEquals(2L, driver.metrics().get("processingErrors"), "a throwing void batch sink fails the whole batch");
      assertEquals(2, driver.errors().size());
      assertTrue(
        driver
          .errors()
          .stream()
          .allMatch(e -> "batch sink down".equals(e.exception().getMessage())),
        "every per-record error carries the batch failure cause"
      );
    }
  }
}
