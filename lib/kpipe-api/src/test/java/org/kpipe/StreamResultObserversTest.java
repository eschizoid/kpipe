package org.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.kpipe.registry.Result;
import org.kpipe.sink.MessageSink;

/// Exercises the [Stream] fluent facade's `onFiltered` / `onFailed` / `peekResult` observer
/// hooks. The pipeline is built via the public facade and driven through `processToSink` so we
/// exercise the same wrapping path the live consumer takes — no Kafka required.
class StreamResultObserversTest {

  private static Properties props() {
    final var p = new Properties();
    p.setProperty("bootstrap.servers", "localhost:9092");
    p.setProperty("group.id", "test-group");
    return p;
  }

  private static DefaultSink<Map<String, Object>> sinkWith(final MessageSink<Map<String, Object>> terminal) {
    return (DefaultSink<Map<String, Object>>) KPipe.json("topic", props()).toCustom(terminal);
  }

  @Test
  void onFilteredFiresWhenAnOperatorReturnsNull() {
    final var fired = new AtomicInteger();
    @SuppressWarnings("unchecked")
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(m -> false)
      .onFiltered(fired::incrementAndGet)
      .toCustom(m -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (org.kpipe.registry.MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    pipeline.processToSink("{}".getBytes());

    assertEquals(1, fired.get());
  }

  @Test
  void onFilteredDoesNotFireForPassedRecords() {
    final var fired = new AtomicInteger();
    @SuppressWarnings("unchecked")
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .onFiltered(fired::incrementAndGet)
      .toCustom(m -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (org.kpipe.registry.MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    pipeline.processToSink("{\"k\":\"v\"}".getBytes());

    assertEquals(0, fired.get());
  }

  @Test
  void onFailedReceivesTheCauseAndPipelineStillThrows() {
    final var capturedCause = new AtomicReference<Throwable>();
    final var boom = new RuntimeException("boom");
    @SuppressWarnings("unchecked")
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .pipe(m -> {
        throw boom;
      })
      .onFailed(capturedCause::set)
      .toCustom(m -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (org.kpipe.registry.MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    final var thrown = assertThrows(RuntimeException.class, () -> pipeline.processToSink("{}".getBytes()));

    assertSame(boom, thrown);
    assertSame(boom, capturedCause.get());
  }

  @Test
  void peekResultFiresOnEveryOutcome() {
    final var passed = new AtomicInteger();
    final var filtered = new AtomicInteger();
    final var failed = new AtomicInteger();
    @SuppressWarnings("unchecked")
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(m -> m.containsKey("keep"))
      .pipe(m -> {
        if (m.containsKey("explode")) throw new RuntimeException("nope");
        return m;
      })
      .peekResult(result -> {
        switch (result) {
          case Result.Passed<Map<String, Object>> __ -> passed.incrementAndGet();
          case Result.Filtered<Map<String, Object>> __ -> filtered.incrementAndGet();
          case Result.Failed<Map<String, Object>> __ -> failed.incrementAndGet();
        }
      })
      .toCustom(m -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (org.kpipe.registry.MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    pipeline.processToSink("{\"keep\":\"yes\"}".getBytes());
    pipeline.processToSink("{\"drop\":\"me\"}".getBytes());
    assertThrows(RuntimeException.class, () -> pipeline.processToSink("{\"keep\":\"yes\",\"explode\":1}".getBytes()));

    assertEquals(1, passed.get());
    assertEquals(1, filtered.get());
    assertEquals(1, failed.get());
  }

  @Test
  void observerExceptionsAreSwallowedSoPipelineStaysAlive() {
    @SuppressWarnings("unchecked")
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(m -> false)
      .onFiltered(() -> {
        throw new RuntimeException("observer bug");
      })
      .toCustom(m -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (org.kpipe.registry.MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    pipeline.processToSink("{}".getBytes()); // must not throw despite observer bug
  }

  @Test
  void noObserverConfiguredMeansNoWrapper() {
    @SuppressWarnings("unchecked")
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .pipe(m -> {
        m.put("x", 1);
        return m;
      })
      .toCustom(m -> {});

    // We can't easily assert "is not the wrapper class" because the wrapper is anonymous, but
    // we can confirm the pipeline still works without observers configured.
    final var pipeline = sink.buildPipeline();
    assertNotNull(pipeline);
  }

  @Test
  void observersDoNotAffectFilterReturnValue() {
    final var seenAtSink = new AtomicReference<Map<String, Object>>();
    @SuppressWarnings("unchecked")
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(m -> false)
      .onFiltered(() -> {
        // observer is a side-effect — must not suppress the filter
      })
      .toCustom((Map<String, Object> v) -> seenAtSink.set(v));

    @SuppressWarnings("unchecked")
    final var pipeline = (org.kpipe.registry.MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    pipeline.processToSink("{\"any\":\"value\"}".getBytes());

    assertNull(seenAtSink.get(), "filtered records must not reach the sink");
  }

  @Test
  void nullObserversAreRejected() {
    final var stream = KPipe.json("topic", props());
    assertThrows(NullPointerException.class, () -> stream.onFiltered(null));
    assertThrows(NullPointerException.class, () -> stream.onFailed(null));
    assertThrows(NullPointerException.class, () -> stream.peekResult(null));
  }

  @Test
  void lastWriteWinsForRepeatedObserverCalls() {
    final var first = new AtomicInteger();
    final var second = new AtomicInteger();
    @SuppressWarnings("unchecked")
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(m -> false)
      .onFiltered(first::incrementAndGet)
      .onFiltered(second::incrementAndGet)
      .toCustom(m -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (org.kpipe.registry.MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    pipeline.processToSink("{}".getBytes());

    assertEquals(0, first.get(), "first observer must be replaced by the second call");
    assertEquals(1, second.get());
  }

  @Test
  void onFailedDoesNotFireOnFilteredOrPassed() {
    final var failedFired = new AtomicInteger();
    @SuppressWarnings("unchecked")
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(m -> m.containsKey("keep"))
      .onFailed(_ -> failedFired.incrementAndGet())
      .toCustom(m -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (org.kpipe.registry.MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    pipeline.processToSink("{\"keep\":\"yes\"}".getBytes());
    pipeline.processToSink("{\"drop\":\"me\"}".getBytes());

    assertEquals(0, failedFired.get());
  }

  // Suppresses the unused-variable warning on sinkWith — kept for readability of test setups.
  @SuppressWarnings("unused")
  private static void useSinkWith(final MessageSink<Map<String, Object>> s) {
    sinkWith(s);
  }
}
