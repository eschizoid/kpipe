package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.channels.ClosedByInterruptException;
import org.junit.jupiter.api.Test;

class InterruptionsTest {

  @Test
  void recognisesAnInterruptionAnywhereInTheCauseChain() {
    assertTrue(Interruptions.isInterruptionRelated(new InterruptedException()));
    assertTrue(Interruptions.isInterruptionRelated(new IllegalStateException(new InterruptedException())));
    assertTrue(Interruptions.isInterruptionRelated(new RuntimeException(new ClosedByInterruptException())));
  }

  @Test
  void anExceptionWithNoInterruptionInItsChainIsNotOne() {
    assertFalse(Interruptions.isInterruptionRelated(new IllegalStateException("bad row")));
    assertFalse(Interruptions.isInterruptionRelated(new RuntimeException(new IllegalArgumentException())));
  }
}
