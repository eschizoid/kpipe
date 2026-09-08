package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// Fray ports of the batch-wrapper buffer-lock races.
///
/// The wrapper's `ReentrantLock` serializes enqueue against flush so parallel-mode workers can
/// buffer records while a flush is mid-flight. `bufferedCount` feeds the in-flight backpressure
/// watermark, so an increment lost to a race understates memory pressure and a decrement that
/// does not match what the flush actually drained strands records against the watermark.
///
/// The wrapper is constructed but never started: the age-tick scheduler is started separately by
/// the consumer, and a live scheduler thread would stop Fray from finishing an iteration. Both
/// scenarios use a one-minute age window so only the size trigger is ever in play.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class BatchWrapperLockFrayTest {

  private static final String TOPIC = "fray-batch";

  /// Two enqueues race with no flush in play — the size threshold is far above the record count,
  /// so both records stay buffered. Each enqueue is one read-modify-write on the counter, and the
  /// lock is what makes the pair add up: any total other than two means an increment was lost or
  /// applied twice.
  @FrayTest(iterations = 500)
  void concurrentEnqueuesAreBothCounted() {
    final var wrapper = newWrapper(new BatchPolicy(1000, Duration.ofMinutes(1)));

    FrayScenarios.runConcurrently(
      () -> wrapper.enqueue(record(1L), record(1L).value()),
      () -> wrapper.enqueue(record(2L), record(2L).value())
    );

    assertEquals(2L, wrapper.bufferedCount(), "both enqueues should be buffered exactly once");
  }

  /// An enqueue races the size-triggered flush that another enqueue sets off. With one record
  /// pre-loaded and a threshold of two, whichever enqueue arrives second trips an inline flush of
  /// two records, leaving exactly one buffered. The flush's decrement has to match the records it
  /// actually drained — decrementing by the wrong amount either strands a record against the
  /// backpressure watermark or double-counts one out of it.
  @FrayTest(iterations = 500)
  void flushDecrementMatchesWhatItDrained() {
    final var wrapper = newWrapper(new BatchPolicy(2, Duration.ofMinutes(1)));
    wrapper.enqueue(record(0L), record(0L).value());

    FrayScenarios.runConcurrently(
      () -> wrapper.enqueue(record(1L), record(1L).value()),
      () -> wrapper.enqueue(record(2L), record(2L).value())
    );

    assertEquals(1L, wrapper.bufferedCount(), "one record should remain buffered after the size-triggered flush");
  }

  private static BatchPipelineWrapper<byte[]> newWrapper(final BatchPolicy policy) {
    return new BatchPipelineWrapper<>(TOPIC, identityPipeline(), succeedingSink(), policy, null, noopCallbacks());
  }

  private static MessagePipeline<byte[]> identityPipeline() {
    return new MessageProcessorRegistry().pipeline(MessageFormat.bytes()).build();
  }

  /// Reports every batch as fully succeeded, so a size-triggered flush runs the real flush path
  /// without routing anything to a failure callback.
  private static BatchSink<byte[]> succeedingSink() {
    return batch -> BatchResult.allSucceeded(batch.size());
  }

  /// The assertions read the wrapper's own `bufferedCount`, not downstream offset bookkeeping,
  /// so the callbacks do nothing.
  private static BatchPipelineWrapper.BatchCallbacks noopCallbacks() {
    return new BatchPipelineWrapper.BatchCallbacks() {
      @Override
      public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {}

      @Override
      public void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {}
    };
  }

  private static ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, ("k-" + offset).getBytes(UTF_8), ("v-" + offset).getBytes(UTF_8));
  }
}
