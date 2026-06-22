package io.github.eschizoid.kpipe.consumer;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
import org.openjdk.jcstress.infra.results.JZ_Result;

/// Concurrency-stress check that a track/mark racing a partition revoke is always a clean no-op.
///
/// A rebalance can revoke a partition (clearing its tracked-offset state and committing its
/// commit point) at the same moment a worker thread is still marking a record processed for that
/// same partition. The property under test: that race never throws, and it never leaves the
/// partition with a commit point that skips a still-pending offset. Whatever survives the race
/// must stay self-consistent — if any offset is still pending, the commit point must stay pinned
/// at it, never jump past it.
///
/// jcstress runs the two actors below against fresh state under every interleaving its scheduler
/// can produce, then evaluates the arbiter once both actors finish. This is the
/// interleaving-verification layer the single-threaded property test cannot reach: the real
/// consumer revokes on the poll thread while virtual-thread workers mark concurrently.
///
/// Scenario. The state tracks offset 100 (pending) on one partition. The revoke actor runs the
/// rebalance listener's revoke path for that partition, which commits and then removes all of its
/// state. The worker actor re-tracks offset 100 (still pending) and marks a higher offset 101
/// processed. Whichever order the two interleave in, offset 100 — when it survives as pending —
/// must hold the commit point at 100; the commit must never advance to 102 while 100 is pending.
///
/// The arbiter reports two values: the partition's current commit point, and whether offset 100
/// is still pending. Acceptable end states are the three the interleavings can legitimately
/// reach: commit pinned at the still-pending 100; everything cleared (`-1`); or the pending entry
/// cleared by the revoke and only the marked 101 surviving (commit 102, nothing pending). The
/// forbidden state is a commit point past 100 while 100 is reported as still pending — that would
/// mean the commit skipped an unprocessed offset.
@JCStressTest
@Outcome(id = "100, true", expect = Expect.ACCEPTABLE, desc = "Commit pinned at the still-pending offset 100.")
@Outcome(id = "-1, false", expect = Expect.ACCEPTABLE, desc = "Revoke cleared all partition state.")
@Outcome(id = "102, false", expect = Expect.ACCEPTABLE, desc = "Pending cleared; only marked offset 101 survived.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "Commit advanced past a still-pending offset, or a throw.")
@State
public class OffsetManagerRevokeRaceJCStressTest {

  private static final String TOPIC = "jcstress-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  private final KafkaOffsetManager<String> manager;
  private final ConsumerRebalanceListener rebalanceListener;

  public OffsetManagerRevokeRaceJCStressTest() {
    final var consumer = new MockConsumer<String, byte[]>(OffsetResetStrategy.EARLIEST);
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    manager = KafkaOffsetManager.builder(consumer).withCommandQueue(commandQueue).build();
    manager.start();
    rebalanceListener = manager.createRebalanceListener();
    // Offset 100 starts pending on the partition: the offset the revoke will race against.
    manager.trackOffset(record(100L));
  }

  @Actor
  public void revoker() {
    rebalanceListener.onPartitionsRevoked(List.of(PARTITION));
  }

  @Actor
  public void worker() {
    manager.trackOffset(record(100L));
    manager.markOffsetProcessed(record(101L));
  }

  @Arbiter
  public void observe(final JZ_Result r) {
    final var partitionState = manager.getPartitionState(PARTITION);
    r.r1 = (long) partitionState.get("nextOffsetToCommit");
    r.r2 = ((int) partitionState.get("pendingCount")) > 0;
  }

  private static ConsumerRecord<String, byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, "k", "v".getBytes());
  }
}
