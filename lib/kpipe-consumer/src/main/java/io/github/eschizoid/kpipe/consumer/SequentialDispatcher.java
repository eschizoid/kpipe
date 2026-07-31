package io.github.eschizoid.kpipe.consumer;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Sequential dispatcher: runs the per-record pipeline inline on the calling thread (the
/// consumer thread). One record at a time, in offset order per partition. No executor, no
/// worker pool, no buffering.
///
/// Pairs with [BackpressureController#lagStrategy] in [KPipeConsumer] — when one record runs
/// at a time, the only meaningful backlog metric is Kafka lag, not in-flight count. Lag-based
/// backpressure does not consult `drainableCount()`.
///
/// `drainableCount()` is still tracked (0 or 1) so `inFlight` metrics and drain reporting stay
/// accurate while a record runs inline. Shutdown correctness is preserved by
/// `KPipeConsumer.close()`'s `thread.join` on the consumer thread.
final class SequentialDispatcher implements Dispatcher {

  private final AtomicLong inFlight = new AtomicLong(0);

  @Override
  public void dispatch(
    final ConsumerRecord<byte[], byte[]> record,
    final Runnable processTask,
    final Runnable onComplete
  ) {
    inFlight.incrementAndGet();
    try {
      processTask.run();
    } finally {
      inFlight.decrementAndGet();
      onComplete.run();
    }
  }

  @Override
  public long drainableCount() {
    return inFlight.get();
  }

  @Override
  public void close() {
    // No resources to release.
  }
}
