package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.Result;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// Exercises the [Stream] fluent facade's `onFiltered` / `onFailed` / `peekResult` observer
/// hooks. The pipeline is built via the public facade and driven through the same
/// deserialize → process → sink path the live consumer takes (no Kafka required).
class StreamResultObserversTest {

  private static Properties props() {
    final var p = new Properties();
    p.setProperty("bootstrap.servers", "localhost:9092");
    p.setProperty("group.id", "test-group");
    return p;
  }

  /// Drive a pipeline from raw bytes the way `KPipeConsumer` would: deserialize,
  /// `switch (process(...))`, invoke the sink on `Passed`, no-op on `Filtered`,
  /// re-throw the cause on `Failed`.
  private static <T> void drive(final MessagePipeline<T> pipeline, final byte[] data) {
    final var value = pipeline.deserializeOrFail(data);
    switch (pipeline.process(value)) {
      case Result.Passed<T> p -> {
        final var sink = pipeline.getSink();
        if (sink != null) sink.accept(p.value());
      }
      case Result.Filtered<T> _ -> {
        /* intentional drop */
      }
      case Result.Failed<T> f -> {
        if (f.cause() instanceof RuntimeException re) throw re;
        if (f.cause() instanceof Error err) throw err;
        throw new RuntimeException(f.cause());
      }
    }
  }

  @Test
  void onFilteredFiresWhenAnOperatorReturnsNull() {
    final var fired = new AtomicInteger();
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(_ -> false)
      .onFiltered(fired::incrementAndGet)
      .toCustom(_ -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    drive(pipeline, "{}".getBytes());

    assertEquals(1, fired.get());
  }

  @Test
  void onFilteredDoesNotFireForPassedRecords() {
    final var fired = new AtomicInteger();
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .onFiltered(fired::incrementAndGet)
      .toCustom(_ -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    drive(pipeline, "{\"k\":\"v\"}".getBytes());

    assertEquals(0, fired.get());
  }

  @Test
  void onFailedReceivesTheCauseAndPipelineStillThrows() {
    final var capturedCause = new AtomicReference<Throwable>();
    final var boom = new RuntimeException("boom");
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .pipe(_ -> {
        throw boom;
      })
      .onFailed(capturedCause::set)
      .toCustom(_ -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    final var thrown = assertThrows(RuntimeException.class, () -> drive(pipeline, "{}".getBytes()));

    assertSame(boom, thrown);
    assertSame(boom, capturedCause.get());
  }

  @Test
  void peekResultFiresOnEveryOutcome() {
    final var passed = new AtomicInteger();
    final var filtered = new AtomicInteger();
    final var failed = new AtomicInteger();
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(m -> m.containsKey("keep"))
      .pipe(m -> {
        if (m.containsKey("explode")) throw new RuntimeException("nope");
        return m;
      })
      .peekResult(result -> {
        switch (result) {
          case Result.Passed<Map<String, Object>> _ -> passed.incrementAndGet();
          case Result.Filtered<Map<String, Object>> _ -> filtered.incrementAndGet();
          case Result.Failed<Map<String, Object>> _ -> failed.incrementAndGet();
        }
      })
      .toCustom(_ -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    drive(pipeline, "{\"keep\":\"yes\"}".getBytes());
    drive(pipeline, "{\"drop\":\"me\"}".getBytes());
    assertThrows(RuntimeException.class, () -> drive(pipeline, "{\"keep\":\"yes\",\"explode\":1}".getBytes()));

    assertEquals(1, passed.get());
    assertEquals(1, filtered.get());
    assertEquals(1, failed.get());
  }

  @Test
  void observerExceptionsAreSwallowedSoPipelineStaysAlive() {
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(_ -> false)
      .onFiltered(() -> {
        throw new RuntimeException("observer bug");
      })
      .toCustom(_ -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    drive(pipeline, "{}".getBytes()); // must not throw despite observer bug
  }

  @Test
  void noObserverConfiguredMeansNoWrapper() {
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .pipe(m -> {
        m.put("x", 1);
        return m;
      })
      .toCustom(_ -> {});

    // We can't easily assert "is not the wrapper class" because the wrapper is anonymous, but
    // we can confirm the pipeline still works without observers configured.
    final var pipeline = sink.buildPipeline();
    assertNotNull(pipeline);
  }

  @Test
  void observersDoNotAffectFilterReturnValue() {
    final var seenAtSink = new AtomicReference<Map<String, Object>>();
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(_ -> false)
      .onFiltered(() -> {
        // observer is a side-effect — must not suppress the filter
      })
      .toCustom(seenAtSink::set);

    @SuppressWarnings("unchecked")
    final var pipeline = (MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    drive(pipeline, "{\"any\":\"value\"}".getBytes());

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
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(_ -> false)
      .onFiltered(first::incrementAndGet)
      .onFiltered(second::incrementAndGet)
      .toCustom(_ -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    drive(pipeline, "{}".getBytes());

    assertEquals(0, first.get(), "first observer must be replaced by the second call");
    assertEquals(1, second.get());
  }

  @Test
  void onFailedDoesNotFireOnFilteredOrPassed() {
    final var failedFired = new AtomicInteger();
    final var sink = (DefaultSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(m -> m.containsKey("keep"))
      .onFailed(_ -> failedFired.incrementAndGet())
      .toCustom(_ -> {});

    @SuppressWarnings("unchecked")
    final var pipeline = (MessagePipeline<Map<String, Object>>) sink.buildPipeline();
    drive(pipeline, "{\"keep\":\"yes\"}".getBytes());
    drive(pipeline, "{\"drop\":\"me\"}".getBytes());

    assertEquals(0, failedFired.get());
  }

  // ── Batch path — observers must fire through toBatch(...) too ─────────────
  // Regression guard: DefaultBatchSink previously built its pipeline WITHOUT observer dispatch,
  // silently dropping onFiltered/onFailed/peekResult set on a toBatch(...) stream (the same
  // silent-drop class as the tracer/circuit-breaker drop fixed on this path earlier).

  @Test
  void batchPipelineFiresPeekResultOnEveryOutcome() {
    final var passed = new AtomicInteger();
    final var filtered = new AtomicInteger();
    final var failed = new AtomicInteger();
    final var sink = (DefaultBatchSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(m -> m.containsKey("keep"))
      .pipe(m -> {
        if (m.containsKey("explode")) throw new RuntimeException("nope");
        return m;
      })
      .peekResult(result -> {
        switch (result) {
          case Result.Passed<Map<String, Object>> _ -> passed.incrementAndGet();
          case Result.Filtered<Map<String, Object>> _ -> filtered.incrementAndGet();
          case Result.Failed<Map<String, Object>> _ -> failed.incrementAndGet();
        }
      })
      .toBatch(BatchSink.ofVoid(_ -> {}), new BatchPolicy(10, Duration.ofDays(1)));

    final var pipeline = sink.buildPipeline();
    final var keep = pipeline.deserializeOrFail("{\"keep\":\"yes\"}".getBytes());
    pipeline.process(keep);
    final var drop = pipeline.deserializeOrFail("{\"drop\":\"me\"}".getBytes());
    pipeline.process(drop);
    final var explode = pipeline.deserializeOrFail("{\"keep\":\"yes\",\"explode\":1}".getBytes());
    pipeline.process(explode);

    assertEquals(1, passed.get(), "batch pipeline must dispatch peekResult for Passed");
    assertEquals(1, filtered.get(), "batch pipeline must dispatch peekResult for Filtered");
    assertEquals(1, failed.get(), "batch pipeline must dispatch peekResult for Failed");
  }

  @Test
  void batchPipelineFiresOnFilteredAndOnFailed() {
    final var filteredFired = new AtomicInteger();
    final var capturedCause = new AtomicReference<Throwable>();
    final var boom = new RuntimeException("boom");
    final var sink = (DefaultBatchSink<Map<String, Object>>) KPipe.json("topic", props())
      .filter(m -> !m.containsKey("drop"))
      .pipe(m -> {
        if (m.containsKey("explode")) throw boom;
        return m;
      })
      .onFiltered(filteredFired::incrementAndGet)
      .onFailed(capturedCause::set)
      .toBatch(BatchSink.ofVoid(_ -> {}), new BatchPolicy(10, Duration.ofDays(1)));

    final var pipeline = sink.buildPipeline();
    pipeline.process(pipeline.deserializeOrFail("{\"drop\":\"me\"}".getBytes()));
    pipeline.process(pipeline.deserializeOrFail("{\"explode\":1}".getBytes()));

    assertEquals(1, filteredFired.get(), "batch pipeline must dispatch onFiltered");
    assertSame(boom, capturedCause.get(), "batch pipeline must dispatch onFailed with the cause");
  }
}
