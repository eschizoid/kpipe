package io.github.eschizoid.kpipe.consumer;

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

/// Concurrency-stress check that the commit point never skips over a still-pending offset.
///
/// jcstress runs the two actors below against fresh state under every interleaving its
/// scheduler can produce, then evaluates the arbiter once both actors have finished. This is
/// the interleaving-verification layer that a sequence-based property test cannot reach: jqwik
/// orders operations on a single thread, whereas the real consumer marks offsets from many
/// virtual-thread workers concurrently.
///
/// Scenario. Three contiguous offsets are tracked across a gap. Offset 100 is tracked in the
/// state constructor and never marked, so it stays pending for the whole run — the permanent
/// gap that holds the commit point down. Two actors then concurrently mark the two higher
/// offsets out of order relative to each other: one marks 102, the other marks 101. Because
/// 100 is still pending, the commit point must stay pinned at 100 no matter how the two mark
/// sequences interleave: it must never jump to 101, 102, 103, or any value past the gap.
///
/// The arbiter reads `nextOffsetToCommit` after both actors complete and reports it as the
/// single result. The only acceptable outcome is 100; any higher value would mean the commit
/// point advanced past an offset whose record had not reached a terminal state.
@JCStressTest
@Outcome(
    id = "100",
    expect = Expect.ACCEPTABLE,
    desc = "Commit point pinned at the still-pending gap offset 100.")
@Outcome(
    id = ".*",
    expect = Expect.FORBIDDEN,
    desc = "Commit point advanced past the still-pending offset 100.")
@State
public class OffsetManagerGapHoldJCStressTest {

  private static final String TOPIC = "jcstress-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  private final KafkaOffsetManager<String> manager;

  public OffsetManagerGapHoldJCStressTest() {
    final var consumer = new MockConsumer<String, byte[]>(OffsetResetStrategy.EARLIEST);
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    manager = KafkaOffsetManager.builder(consumer).withCommandQueue(commandQueue).build();
    manager.start();
    // Offset 100 is tracked but never marked: the permanent gap pinning the commit point.
    manager.trackOffset(record(100L));
  }

  @Actor
  public void marker102() {
    manager.trackOffset(record(102L));
    manager.markOffsetProcessed(record(102L));
  }

  @Actor
  public void marker101() {
    manager.trackOffset(record(101L));
    manager.markOffsetProcessed(record(101L));
  }

  @Arbiter
  public void observe(final J_Result r) {
    r.r1 = (long) manager.getPartitionState(PARTITION).get("nextOffsetToCommit");
  }

  private static ConsumerRecord<String, byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, "k", "v".getBytes());
  }
}
