package io.github.eschizoid.kpipe.consumer;

import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.J_Result;

/// Concurrency-stress check for the check-then-act safety of reading the lowest pending offset of
/// `KafkaOffsetManager` while another thread empties the per-partition pending set.
///
/// The manager keeps pending offsets per partition in a `ConcurrentSkipListSet`, and computes a
/// partition's commit point by reading the set's first element. A naive `if (!set.isEmpty())
/// set.first()` is a check-then-act trap: a concurrent remover can drain the set between the
/// emptiness check and the `first()` call, and `first()` then throws `NoSuchElementException` on
/// an empty set. The manager guards this with a `safeFirst` wrapper that catches that exception
/// and returns null, so the commit-point read degrades to "no pending offset" rather than blowing
/// up the caller. This test races a remover against a reader to exercise exactly that window.
///
/// Scenario. The state constructor tracks a single offset (100) on one partition, so the pending
/// set starts holding exactly that one element — the set most likely to be observed mid-drain.
/// The remover actor marks offset 100 processed, which removes it from the set and clears the
/// partition entry once empty. The reader actor reads the commit point via
/// `getPartitionState(...).get("nextOffsetToCommit")`, which routes through `safeFirst`. The two
/// run concurrently under every interleaving the scheduler can produce.
///
/// The property under test is narrow and exact: `safeFirst` must never let a
/// `NoSuchElementException` escape, no matter how the remover drains the set relative to the
/// reader's emptiness-check-then-first call. jcstress surfaces a thrown actor as a hard error, so a
/// leaked exception fails the run outright — it never reaches the outcome grading below. Every
/// graded outcome therefore already proves the catch held; the values just describe which snapshot
/// the reader happened to see.
///
/// The reader records the commit point it observed. 101 is the common case (set drained, commit
/// point falls back to highest-processed + 1) and 100 is the case where the read landed before the
/// remove. A third value, -1, is also reachable and is deliberately marked interesting rather than
/// forbidden: the reader's snapshot reads the pending set and the highest-processed map in two
/// separate steps, and the remover updates those two maps non-atomically, so the reader can observe
/// the set already emptied while the highest-processed write is not yet visible. That torn read is
/// a benign staleness of the diagnostic snapshot, not a `safeFirst` failure — `safeFirst` still
/// returned null without throwing. The only forbidden behaviour, a thrown exception, cannot appear
/// here precisely because jcstress would have failed the run before grading.
@JCStressTest
@Outcome(id = "100", expect = Expect.ACCEPTABLE, desc = "Reader saw offset 100 still pending.")
@Outcome(id = "101", expect = Expect.ACCEPTABLE, desc = "Reader saw the set drained; commit point is processed + 1.")
@Outcome(
  id = "-1",
  expect = Expect.ACCEPTABLE_INTERESTING,
  desc = "Torn snapshot: set seen drained before the highest-processed write was visible. safeFirst still did not throw."
)
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "Unexpected commit-point value.")
@State
public class SafeFirstJCStressTest {

  private static final String TOPIC = "jcstress-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  private final KafkaOffsetManager<String> manager;

  public SafeFirstJCStressTest() {
    final var consumer = new MockConsumer<String, byte[]>(OffsetResetStrategy.EARLIEST);
    final var commandQueue = new LinkedBlockingQueue<ConsumerCommand>();
    manager = KafkaOffsetManager.builder(consumer).withCommandQueue(commandQueue).build();
    manager.start();
    // Single tracked offset: the pending set holds exactly {100} when the actors begin.
    manager.trackOffset(record(100L));
  }

  @Actor
  public void remover() {
    manager.markOffsetProcessed(record(100L));
  }

  @Actor
  public void reader(final J_Result r) {
    r.r1 = (long) manager.getPartitionState(PARTITION).get("nextOffsetToCommit");
  }

  private static ConsumerRecord<String, byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, "k", "v".getBytes());
  }
}
