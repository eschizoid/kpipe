package org.kpipe.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

/// Contract-level tests for [BatchResult]. Focuses on what the [BatchPipelineWrapper] relies on:
/// factory output covers the right index range, defensive copies make caller mutation harmless,
/// and the error-attribution invariant in `allFailed` (every index → the same cause).
class BatchResultTest {

  @Test
  void allSucceededCoversExactlyZeroToBatchSize() {
    final var result = BatchResult.allSucceeded(5);

    assertEquals(List.of(0, 1, 2, 3, 4), result.succeededIndexes());
    assertTrue(result.failedByIndex().isEmpty());
  }

  @Test
  void allSucceededAllowsZeroSize() {
    // Empty batches are legal — a flush of nothing.
    final var result = BatchResult.allSucceeded(0);

    assertTrue(result.succeededIndexes().isEmpty());
    assertTrue(result.failedByIndex().isEmpty());
  }

  @Test
  void allFailedAttributesEveryIndexToTheSameCause() {
    final var cause = new RuntimeException("connection pool died");

    final var result = BatchResult.allFailed(3, cause);

    assertTrue(result.succeededIndexes().isEmpty());
    assertEquals(3, result.failedByIndex().size());
    for (int i = 0; i < 3; i++) assertSame(
      cause,
      result.failedByIndex().get(i),
      "index " + i + " must point to the same cause"
    );
  }

  @Test
  void factoriesRejectNegativeBatchSize() {
    assertThrows(IllegalArgumentException.class, () -> BatchResult.allSucceeded(-1));
    assertThrows(IllegalArgumentException.class, () -> BatchResult.allFailed(-1, new RuntimeException()));
  }

  @Test
  void canonicalConstructorDefensivelyCopies() {
    // Mutating the caller-supplied collections after construction must not affect the BatchResult.
    // The wrapper relies on this — it walks the result while the caller may still hold references.
    final var indexes = new ArrayList<Integer>();
    indexes.add(0);
    indexes.add(1);
    final var failures = new HashMap<Integer, Exception>();
    failures.put(2, new RuntimeException("row 2"));

    final var result = new BatchResult(indexes, failures);

    indexes.clear();
    failures.clear();

    assertEquals(List.of(0, 1), result.succeededIndexes());
    assertEquals(1, result.failedByIndex().size());
  }
}
