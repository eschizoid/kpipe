package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchResult;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.J_Result;

/// Concurrency-stress checks that the lock-guarded buffer inside the batch wrapper keeps its
/// `bufferedCount` gauge consistent when two enqueues race — never losing a record, never
/// double-counting one, never going negative.
///
/// jcstress drives the two actors below against fresh state under every interleaving its
/// scheduler can produce, then evaluates the arbiter once both actors have finished. The wrapper
/// is the real production class: it is package-private and constructed directly here from the same
/// package, with a no-op sink and a one-minute age window so the only flush trigger in play is the
/// size threshold. This nested-class file holds two independent stress tests, one per scenario.
public class BatchWrapperLockJCStressTest {

  private static final String TOPIC = "jcstress-batch";

  private static ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, ("k-" + offset).getBytes(UTF_8), ("v-" + offset).getBytes());
  }

  /// A sink that always reports the whole batch as succeeded. Used so a size-triggered flush runs
  /// the real flush path without routing anything to a failure callback.
  private static BatchSink<byte[]> succeedingSink() {
    return batch -> BatchResult.allSucceeded(batch.size());
  }

  /// Callbacks that do nothing — the tests assert on the wrapper's own `bufferedCount`, not on
  /// downstream offset bookkeeping.
  private static BatchPipelineWrapper.BatchCallbacks noopCallbacks() {
    return new BatchPipelineWrapper.BatchCallbacks() {
      @Override
      public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {}

      @Override
      public void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {}
    };
  }

  /// Two enqueues race with no flush in play. The size threshold is high and the age window is
  /// long, so neither enqueue trips a flush — both records stay buffered. After both actors
  /// finish, `bufferedCount` must be exactly 2: one increment per enqueue, the lock serializing
  /// the two read-modify-write sequences. Any other value means an increment was lost to a race
  /// or applied twice.
  @JCStressTest
  @Outcome(id = "2", expect = Expect.ACCEPTABLE, desc = "Both enqueues counted exactly once.")
  @Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "An enqueue increment was lost or double-counted.")
  @State
  public static class EnqueueEnqueue {

    private final BatchPipelineWrapper<byte[]> wrapper;

    public EnqueueEnqueue() {
      // High size threshold and long age window: no enqueue trips a flush, so both records
      // remain buffered for the arbiter to count.
      final var policy = new BatchPolicy(1000, Duration.ofMinutes(1));
      wrapper = new BatchPipelineWrapper<>(
        TOPIC,
        TestPipelines.identity(),
        succeedingSink(),
        policy,
        null,
        noopCallbacks()
      );
    }

    @Actor
    public void enqueueA() {
      wrapper.enqueue(record(1L), record(1L).value());
    }

    @Actor
    public void enqueueB() {
      wrapper.enqueue(record(2L), record(2L).value());
    }

    @Arbiter
    public void observe(final J_Result r) {
      r.r1 = wrapper.bufferedCount();
    }
  }

  /// An enqueue races a size-triggered flush. The buffer is pre-loaded with one record and the
  /// size threshold is 2, so whichever enqueue takes the buffer from 1 to 2 trips an inline flush
  /// that drains both buffered records and decrements `bufferedCount` by the flushed size. The
  /// lock serializes enqueue against flush, so the two legal serial orders both leave exactly one
  /// record buffered. The final `bufferedCount` must therefore be 1: the flush decrement must
  /// match the count it removed, with no lost or stranded record.
  @JCStressTest
  @Outcome(
    id = "1",
    expect = Expect.ACCEPTABLE,
    desc = "Flush decrement matched the records it drained; one left buffered."
  )
  @Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "Flush lost, stranded, or double-counted a record.")
  @State
  public static class EnqueueVersusFlush {

    private final BatchPipelineWrapper<byte[]> wrapper;

    public EnqueueVersusFlush() {
      // Size threshold 2 with one record pre-loaded: the next enqueue trips an inline flush.
      final var policy = new BatchPolicy(2, Duration.ofMinutes(1));
      wrapper = new BatchPipelineWrapper<>(
        TOPIC,
        TestPipelines.identity(),
        succeedingSink(),
        policy,
        null,
        noopCallbacks()
      );
      wrapper.enqueue(record(0L), record(0L).value());
    }

    @Actor
    public void enqueueA() {
      wrapper.enqueue(record(1L), record(1L).value());
    }

    @Actor
    public void enqueueB() {
      wrapper.enqueue(record(2L), record(2L).value());
    }

    @Arbiter
    public void observe(final J_Result r) {
      r.r1 = wrapper.bufferedCount();
    }
  }
}
