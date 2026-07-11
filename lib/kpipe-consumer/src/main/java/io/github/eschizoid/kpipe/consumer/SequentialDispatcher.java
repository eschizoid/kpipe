package io.github.eschizoid.kpipe.consumer;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Sequential dispatcher: runs the per-record pipeline inline on the calling thread (the
/// consumer thread). One record at a time, in offset order per partition. No executor, no
/// worker pool, no buffering.
///
/// Pairs with [BackpressureController#lagStrategy] in [KPipeConsumer] — when one record runs
/// at a time, the only meaningful backlog metric is Kafka lag, not in-flight count. Lag-based
/// backpressure does not consult `pendingCount()`.
///
/// **Why we still track an in-flight count even though dispatch is inline.** The counter is
/// reported as `inFlight` in [KPipeConsumer#getMetrics] (OTel + user-visible) and feeds the
/// `totalInFlight()` reading used by `shutdownGracefully(timeout)` for drain reporting.
/// Without tracking, both would read 0 even while a record is actively processing, which is
/// misleading. The value is always 0 or 1 (a single consumer thread, one inline record at a
/// time), but it's accurate.
///
/// Shutdown correctness is preserved by `KPipeConsumer.close()`'s `thread.join` on the
/// consumer thread — the join waits for any in-flight `processTask.run()` to return regardless
/// of `pendingCount()`. PARALLEL and KEY_ORDERED dispatch onto distinct virtual threads, so
/// they need a real counter to participate in `waitForInFlightDrain` too.
final class SequentialDispatcher implements Dispatcher {

  private final AtomicLong inFlight = new AtomicLong(0);

  @Override
  public void dispatch(final ConsumerRecord<byte[], byte[]> record, final Runnable processTask, final Runnable onComplete) {
    inFlight.incrementAndGet();
    try {
      processTask.run();
    } finally {
      inFlight.decrementAndGet();
      onComplete.run();
    }
  }

  @Override
  public long pendingCount() {
    return inFlight.get();
  }

  @Override
  public void close() {
    // No resources to release.
  }
}
