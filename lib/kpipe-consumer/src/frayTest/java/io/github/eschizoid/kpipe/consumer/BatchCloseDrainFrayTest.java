package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// `close()` must not return while an outcome dispatch is still to run.
///
/// The flush hands its per-record dispatch back to the caller to run after the lock is released,
/// so the lock alone no longer holds `close()` back the way it did when the dispatch ran inside
/// it. `close()` compensates by waiting on a count of dispatches still outstanding, and that count
/// is incremented while the flush lock is **still held** rather than when the dispatch begins.
///
/// The placement is the whole point, and it is invisible to ordinary thread tests. A test that
/// closes after a callback has already fired never enters the window the placement exists to
/// cover, and so passes with the increment in either position. The window is between the flush's
/// `unlock()` and the dispatch's first statement — a handful of instructions that a racing test
/// hits by luck, and that Fray schedules deliberately.
///
/// The wrapper is constructed but never started: a live age-tick scheduler thread would stop Fray
/// finishing an iteration, and the one-minute age window keeps the size trigger the only one in
/// play.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class BatchCloseDrainFrayTest {

  private static final String TOPIC = "fray-close-drain";

  /// One enqueue trips a size flush while another thread closes the wrapper.
  ///
  /// The assertion is an ordering one rather than a count. Sampling "callbacks still running" the
  /// instant `close()` returns reads zero in exactly the broken interleaving — `close()` slips
  /// past *before* the dispatch starts, so nothing is running yet and the sample looks clean. What
  /// distinguishes the two placements is whether any callback runs *after* `close()` has returned,
  /// which is what a flag set on return and read inside the callback captures.
  @FrayTest(iterations = 500)
  void noOutcomeCallbackRunsAfterCloseReturns() {
    final var closeReturned = new AtomicBoolean();
    final var callbackRanAfterClose = new AtomicBoolean();

    final var wrapper = newWrapper(new BatchPolicy(2, Duration.ofMinutes(1)), closeReturned, callbackRanAfterClose);
    // One short of the threshold, so the racing enqueue below is what trips the flush.
    wrapper.enqueue(record(0L), record(0L).value());

    FrayScenarios.runConcurrently(
      () -> wrapper.enqueue(record(1L), record(1L).value()),
      () -> {
        wrapper.close();
        closeReturned.set(true);
      }
    );

    assertFalse(
      callbackRanAfterClose.get(),
      "an outcome callback ran after close() returned; the consumer tears the offset manager and " +
        "DLQ producer down around it"
    );
  }

  private static BatchPipelineWrapper<byte[]> newWrapper(
    final BatchPolicy policy,
    final AtomicBoolean closeReturned,
    final AtomicBoolean callbackRanAfterClose
  ) {
    final var callbacks = new BatchPipelineWrapper.BatchCallbacks() {
      @Override
      public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {
        if (closeReturned.get()) callbackRanAfterClose.set(true);
      }

      @Override
      public void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {
        if (closeReturned.get()) callbackRanAfterClose.set(true);
      }
    };
    return new BatchPipelineWrapper<>(TOPIC, identityPipeline(), succeedingSink(), policy, null, callbacks);
  }

  private static MessagePipeline<byte[]> identityPipeline() {
    return new MessageProcessorRegistry().pipeline(MessageFormat.bytes()).build();
  }

  private static BatchSink<byte[]> succeedingSink() {
    return batch -> BatchResult.allSucceeded(batch.size());
  }

  private static ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, ("k-" + offset).getBytes(UTF_8), ("v-" + offset).getBytes(UTF_8));
  }
}
