package io.github.eschizoid.kpipe.consumer;

import java.util.concurrent.atomic.AtomicReference;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZL_Result;

/// Concurrency-stress check for the single-read compare-and-set the consumer uses to move its
/// lifecycle state toward shutdown.
///
/// The consumer drives an `AtomicReference<ConsumerState>` toward `CLOSING` from any active state
/// (`RUNNING` or `PAUSED`). An external `close()` and a self-terminating consumer thread can race
/// to perform that transition. The transition must be done by reading the current state ONCE into
/// a local, deciding whether it is active, then attempting a single `compareAndSet` against that
/// captured value. Reading the field twice (a check-then-act, or two sequential CAS calls) opens a
/// window where both racers believe they performed the move, which would double-fire shutdown
/// work that is meant to run exactly once.
///
/// This @State holds a fresh `AtomicReference<ConsumerState>` initialized to `RUNNING` and drives
/// the REAL transition logic — `KPipeConsumer.tryTransitionToClosing`, the static helper the
/// instance method delegates to — not a copy, so the discipline is verified against the shipping
/// code. The two actors run that helper concurrently. The property: exactly ONE actor observes a
/// successful transition (one returns true, the other false), and the final state is `CLOSING`. A
/// run where both actors return true (or both false, or the final state is anything other than
/// `CLOSING`) means the single-read-CAS discipline failed to make the transition a single-winner
/// operation.
///
/// jcstress runs the actors against fresh state under every interleaving its scheduler can produce,
/// then evaluates the arbiter once both have finished. r1/r2 carry each actor's CAS result and r3
/// carries the final state.
@JCStressTest
@Outcome(
  id = "true, false, CLOSING",
  expect = Expect.ACCEPTABLE,
  desc = "First actor won the transition; state is CLOSING."
)
@Outcome(
  id = "false, true, CLOSING",
  expect = Expect.ACCEPTABLE,
  desc = "Second actor won the transition; state is CLOSING."
)
@Outcome(
  id = ".*",
  expect = Expect.FORBIDDEN,
  desc = "Both actors won, neither won, or the final state is not CLOSING."
)
@State
public class StateTransitionCasJCStressTest {

  private final AtomicReference<ConsumerState> state = new AtomicReference<>(ConsumerState.RUNNING);

  @Actor
  public void closer(final ZZL_Result r) {
    r.r1 = KPipeConsumer.tryTransitionToClosing(state);
  }

  @Actor
  public void selfTerminator(final ZZL_Result r) {
    r.r2 = KPipeConsumer.tryTransitionToClosing(state);
  }

  @Arbiter
  public void observe(final ZZL_Result r) {
    r.r3 = state.get();
  }
}
