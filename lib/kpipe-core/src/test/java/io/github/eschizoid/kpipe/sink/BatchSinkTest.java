package io.github.eschizoid.kpipe.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/// Contract tests for [BatchSink#ofVoid]. The wrapper has to be honest about what happened:
/// normal completion ⇒ all-succeeded, exception ⇒ all-failed with the *same* exception attached
/// to every index. The downstream `BatchPipelineWrapper` depends on both invariants — drop the
/// exception identity and DLQ messages lose their cause; turn a normal return into a fake failure
/// and offsets stop committing.
class BatchSinkTest {

  @Test
  void ofVoidNormalCompletionMapsToAllSucceeded() {
    final var seen = new AtomicReference<List<String>>();
    final BatchSink<String> sink = BatchSink.ofVoid(seen::set);

    final var result = sink.apply(List.of("a", "b", "c"));

    assertEquals(List.of("a", "b", "c"), seen.get(), "consumer must receive the original batch");
    assertEquals(List.of(0, 1, 2), result.succeededIndexes());
    assertTrue(result.failedByIndex().isEmpty());
  }

  @Test
  void ofVoidThrownExceptionMapsToAllFailedWithSameCause() {
    final var boom = new RuntimeException("DB unavailable");
    final BatchSink<String> sink = BatchSink.ofVoid(_ -> {
      throw boom;
    });

    final var result = sink.apply(List.of("a", "b"));

    assertTrue(result.succeededIndexes().isEmpty());
    assertEquals(2, result.failedByIndex().size());
    assertSame(boom, result.failedByIndex().get(0));
    assertSame(boom, result.failedByIndex().get(1));
  }

  @Test
  void ofVoidEmptyBatchStillRoundTrips() {
    // An empty batch must not error out — the wrapper may call the sink with an empty list during
    // edge-of-window flushes (rare but legal).
    final BatchSink<String> sink = BatchSink.ofVoid(_ -> {});

    final var result = sink.apply(List.of());

    assertTrue(result.succeededIndexes().isEmpty());
    assertTrue(result.failedByIndex().isEmpty());
  }
}
