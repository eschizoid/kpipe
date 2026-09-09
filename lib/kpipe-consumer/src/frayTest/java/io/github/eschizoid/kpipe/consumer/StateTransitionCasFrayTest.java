package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// The consumer state-machine CAS race.
///
/// Two paths reach the same transition: an external `close()` and the consumer thread's own
/// uncaught-exception handler self-terminating. Both call the shared helper, and exactly one
/// must win — a transition that let both callers through would run the shutdown sequence twice,
/// and one that let neither through would leave the consumer wedged in RUNNING with nothing
/// driving it to a terminal state.
///
/// The property is a single-read compare-and-set: read the state once into a local, decide, then
/// CAS. Two sequential CAS calls would open a window where both callers observe RUNNING and both
/// believe they won.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class StateTransitionCasFrayTest {

  @FrayTest(iterations = 300)
  void exactlyOneCallerWinsTheTransitionToClosing() {
    final var state = new AtomicReference<>(ConsumerState.RUNNING);
    final var closerWon = new AtomicBoolean();
    final var selfTerminatorWon = new AtomicBoolean();

    FrayScenarios.runConcurrently(
      () -> closerWon.set(KPipeConsumer.tryTransitionToClosing(state)),
      () -> selfTerminatorWon.set(KPipeConsumer.tryTransitionToClosing(state))
    );

    assertTrue(
      closerWon.get() ^ selfTerminatorWon.get(),
      () ->
        "exactly one caller must win the transition, but closer=" +
        closerWon.get() +
        " and selfTerminator=" +
        selfTerminatorWon.get()
    );
    assertEquals(ConsumerState.CLOSING, state.get(), "the state must land in CLOSING either way");
  }
}
