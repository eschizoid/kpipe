package io.github.eschizoid.kpipe.sink;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.J_Result;

/// Concurrency-stress check that the composite drops no fanout delegation under concurrent sends.
///
/// [CompositeMessageSink] is itself stateless: it holds an immutable copied list of sinks and its
/// `accept` only iterates and delegates, with no shared mutable accumulator of its own. There is
/// therefore no internal race to pin — the composite has no field a concurrent send could corrupt.
/// What is worth pinning is the delivery guarantee across that stateless delegation: when two
/// threads call `accept` on the same composite at the same time, every downstream sink must still
/// observe both values. This wires a single shared downstream sink that counts invocations on an
/// [AtomicLong]; two concurrent `accept` calls must produce exactly two downstream invocations.
///
/// The arbiter reads the downstream count after both actors complete. The only acceptable outcome
/// is 2; a lower value would mean the composite swallowed a delegation under concurrency. (The
/// shared sink uses an atomic counter precisely so the check measures the composite's fanout, not
/// a lost-update race in the downstream sink itself.)
@JCStressTest
@Outcome(id = "2", expect = Expect.ACCEPTABLE, desc = "Both concurrent sends fanned out to the shared sink.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "A fanout delegation was dropped under concurrency.")
@State
public class CompositeMessageSinkJCStressTest {

  private final AtomicLong downstreamInvocations = new AtomicLong();
  private final CompositeMessageSink<String> composite;

  public CompositeMessageSinkJCStressTest() {
    final MessageSink<String> counting = _ -> downstreamInvocations.incrementAndGet();
    composite = new CompositeMessageSink<>(List.of(counting));
  }

  @Actor
  public void senderA() {
    composite.accept("a");
  }

  @Actor
  public void senderB() {
    composite.accept("b");
  }

  @Arbiter
  public void observe(final J_Result r) {
    r.r1 = downstreamInvocations.get();
  }
}
