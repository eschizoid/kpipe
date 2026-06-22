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
import org.openjdk.jcstress.infra.results.JJ_Result;

/// Concurrency-stress check that tracking a new offset never gets silently lost when it races the
/// remove-if-empty that retires the last pending offset on the same partition.
///
/// This is the exact production race: `trackOffset` runs on the poll thread while
/// `markOffsetProcessed` runs on worker virtual threads for the same partition. The pending-offset
/// set per partition is held in a `ConcurrentHashMap`; retiring the last offset empties the set and
/// atomically drops the key. If tracking a fresh offset does its add outside that same bucket lock,
/// the add can land on a set that was concurrently emptied and removed from the map — orphaned —
/// silently losing the offset and letting the commit point advance past an in-flight record.
///
/// Scenario. The state constructor tracks offset 100 (pending). One actor retires it
/// (`markOffsetProcessed(100)`), emptying the set and firing the remove-if-empty. The other actor
/// tracks a fresh offset 200 on the same partition. After both finish, 200 must still be pending:
/// the commit point is 200 and the pending count is 1. The losing interleaving — where 200's add
/// hits an orphaned set — leaves the partition with nothing pending, so the commit point falls back
/// to the retired offset's successor (101) with a pending count of 0.
@JCStressTest
@Outcome(id = "200, 1", expect = Expect.ACCEPTABLE, desc = "Freshly tracked offset 200 survived the concurrent retire.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "Tracked offset 200 was lost to the concurrent remove-if-empty.")
@State
public class RemoveIfEmptyJCStressTest {

  private static final String TOPIC = "jcstress-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  private final KafkaOffsetManager<String> manager;

  public RemoveIfEmptyJCStressTest() {
    final var consumer = new MockConsumer<String, byte[]>(OffsetResetStrategy.EARLIEST);
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    manager = KafkaOffsetManager.builder(consumer).withCommandQueue(commandQueue).build();
    manager.start();
    // Offset 100 is the only pending offset; retiring it empties the set and drops the key.
    manager.trackOffset(record(100L));
  }

  @Actor
  public void retire() {
    manager.markOffsetProcessed(record(100L));
  }

  @Actor
  public void track() {
    manager.trackOffset(record(200L));
  }

  @Arbiter
  public void observe(final JJ_Result r) {
    final var state = manager.getPartitionState(PARTITION);
    r.r1 = (long) state.get("nextOffsetToCommit");
    r.r2 = ((Number) state.get("pendingCount")).longValue();
  }

  private static ConsumerRecord<String, byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, "k", "v".getBytes());
  }
}
