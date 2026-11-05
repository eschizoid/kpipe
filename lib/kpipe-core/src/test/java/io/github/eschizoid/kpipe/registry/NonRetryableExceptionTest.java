package io.github.eschizoid.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class NonRetryableExceptionTest {

  @Test
  void keepsItsMessageAndCause() {
    final var cause = new IllegalStateException("truncated");
    final var failure = new NonRetryableException("malformed", cause);

    assertEquals("malformed", failure.getMessage());
    assertSame(cause, failure.getCause());
    assertNull(new NonRetryableException("missing field").getCause());
  }

  @Test
  void aNonRetryableFailureIsRecognised() {
    assertTrue(NonRetryableException.isNonRetryable(new NonRetryableException("missing field")));
  }

  @Test
  void aFailureCausedByOneIsRecognised() {
    final var wrapped = new RuntimeException(
      "outer",
      new IllegalStateException("middle", new NonRetryableException("x"))
    );

    assertTrue(NonRetryableException.isNonRetryable(wrapped));
  }

  @Test
  void anOrdinaryFailureIsNot() {
    assertFalse(NonRetryableException.isNonRetryable(new RuntimeException("timeout", new IllegalStateException())));
    assertFalse(NonRetryableException.isNonRetryable(null));
  }

  @Test
  void aSuppressedNonRetryableFailureDoesNotStopRetries() {
    final var transientFailure = new RuntimeException("timeout");
    transientFailure.addSuppressed(new NonRetryableException("close failed"));

    assertFalse(NonRetryableException.isNonRetryable(transientFailure));
  }

  @Test
  void aCauseCycleEnds() {
    final var first = new RuntimeException("first");
    final var second = new RuntimeException("second", first);
    first.initCause(second);

    assertFalse(NonRetryableException.isNonRetryable(first));
  }
}
