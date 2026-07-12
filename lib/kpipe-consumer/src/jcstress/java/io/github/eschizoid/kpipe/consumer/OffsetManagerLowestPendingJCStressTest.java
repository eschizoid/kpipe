package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.J_Result;

/// Concurrency-stress check for the lowest-pending invariant of `KafkaOffsetManager`.
///
/// jcstress runs the two actors below against fresh state under every interleaving its scheduler
/// can produce, then evaluates the arbiter once both actors have finished. This is the
/// interleaving-verification layer that the property test cannot reach: jqwik sequences operations
/// on a single thread, whereas the real consumer marks offsets from many virtual-thread workers
/// concurrently.
///
/// Scenario. Offset 100 is tracked in the state constructor and never marked, so it stays pending
/// for the whole run — the permanent gap. Two actors then concurrently track-and-mark two higher
/// offsets (102 and 103) out of order relative to each other. Because 100 is still pending, the
/// commit point must stay pinned at 100 no matter how the two track/mark sequences interleave: it
/// must never jump to 103, 104, or any value past the gap.
///
/// The arbiter reads `nextOffsetToCommit` after both actors complete and reports it as the single
/// result. The only acceptable outcome is 100.
@JCStressTest
@Outcome(id = "100", expect = Expect.ACCEPTABLE, desc = "Commit point pinned at the still-pending gap offset 100.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "Commit point advanced past the still-pending offset 100.")
@State
public class OffsetManagerLowestPendingJCStressTest {

  private static final String TOPIC = "jcstress-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  private final KafkaOffsetManager manager;

  public OffsetManagerLowestPendingJCStressTest() {
    final var consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    manager = KafkaOffsetManager.builder(consumer).withCommandQueue(commandQueue).build();
    manager.start();
    // Offset 100 is tracked but never marked: it is the permanent gap that pins the commit point.
    manager.trackOffset(record(100L));
  }

  @Actor
  public void marker102() {
    manager.trackOffset(record(102L));
    manager.markOffsetProcessed(record(102L));
  }

  @Actor
  public void marker103() {
    manager.trackOffset(record(103L));
    manager.markOffsetProcessed(record(103L));
  }

  @Arbiter
  public void observe(final J_Result r) {
    r.r1 = (long) manager.getPartitionState(PARTITION).get("nextOffsetToCommit");
  }

  private static ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, "k".getBytes(UTF_8), "v".getBytes());
  }
}
