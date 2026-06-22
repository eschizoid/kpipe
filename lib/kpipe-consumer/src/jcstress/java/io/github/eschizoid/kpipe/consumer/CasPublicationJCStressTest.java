package io.github.eschizoid.kpipe.consumer;

import java.util.concurrent.atomic.AtomicReference;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/// Concurrency-stress check for safe publication of a field a starting thread hands to a thread
/// that races it through the lifecycle reference.
///
/// When the consumer starts, it builds the worker thread, then publishes that worker through an
/// atomic lifecycle reference; concurrently a `close()` (or the worker's own teardown) reads that
/// reference to act on the worker. The publication discipline this pins: the payload field is
/// fully written BEFORE the releasing store into the atomic reference, and a reader that performs
/// the acquiring read of the reference and finds the published handle is then guaranteed to see the
/// complete payload — never a stale, half-published default. The releasing store carries every
/// write that program-order-precedes it across to any thread that observes that store.
///
/// The dangerous inversion the consumer deliberately avoids — storing the reference (or flipping a
/// plain state flag) FIRST and assigning the payload AFTER — gives a reader that observes the store
/// no happens-before edge to the later write, so it can read the stale default even though the
/// store is visible. The fix is the ordering enforced here plus publishing through the atomic
/// reference rather than a plain field. This test pins the correct ordering as a regression guard.
///
/// This @State replicates the publication shape on a minimal pair of fields rather than the full
/// consumer, which cannot be built inside a jcstress @State. The writer assigns the payload, then
/// performs the releasing store into the atomic handle. The reader reads the handle once, then
/// reads the payload once.
///
/// jcstress drives the actors against fresh state under every interleaving its scheduler can
/// produce. r1 records whether the reader observed the published handle (1) or the empty initial
/// handle (0); r2 records the payload it then saw (7 = published, 0 = stale default). The forbidden
/// outcome is "reader observed the published handle but the payload was still the stale default" —
/// the broken-publication signature. The other shapes are legal orderings where the reader simply
/// ran before the writer's releasing store.
@JCStressTest
@Outcome(
  id = "1, 7",
  expect = Expect.ACCEPTABLE,
  desc = "Reader observed the published handle and the payload — safe publication held."
)
@Outcome(
  id = "0, 0",
  expect = Expect.ACCEPTABLE,
  desc = "Reader ran before the releasing store — empty handle, default payload."
)
@Outcome(
  id = "0, 7",
  expect = Expect.ACCEPTABLE,
  // Legal and benign: the reader never observed the publication (empty handle, r1=0), so the
  // publication contract makes no promise about the payload here. Its plain read simply raced
  // ahead and saw the payload write without the handle store. This is NOT a publication failure
  // — that signature is the r1=1 case, guarded as FORBIDDEN below. Reachable on weakly-ordered
  // CPUs (where the payload write can become visible before the handle store), legal on all.
  desc = "Reader did not observe the publication (empty handle); its plain payload read raced ahead."
)
@Outcome(
  id = "1, 0",
  expect = Expect.FORBIDDEN,
  desc = "Reader observed the published handle but the payload was still stale — broken publication."
)
@State
public class CasPublicationJCStressTest {

  /// Sentinel for the published handle the writer stores after the payload is written.
  private static final Object HANDLE = new Object();

  /// Sentinel for the payload value the writer writes before publishing the handle.
  private static final int PUBLISHED = 7;

  /// Lifecycle handle, mirroring the consumer's atomic worker-thread reference. The releasing store
  /// into this reference is what carries the payload write across to an observing reader.
  private final AtomicReference<Object> handle = new AtomicReference<>();

  /// Payload assigned before the releasing store. Read directly by a thread that observed the
  /// handle, the way the consumer's teardown reads fields it needs once it sees the worker.
  private int payload = 0;

  @Actor
  public void writer() {
    payload = PUBLISHED;
    handle.set(HANDLE);
  }

  @Actor
  public void reader(final II_Result r) {
    r.r1 = handle.get() == HANDLE ? 1 : 0;
    r.r2 = payload;
  }
}
