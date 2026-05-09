package org.kpipe.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/// Outcome of a [BatchSink#apply] invocation. Reports per-record success and failure by
/// **index into the input batch** rather than by value, because the deserialized value type `T`
/// may not have stable equals/hashCode and the consumer needs to tie each result back to the
/// originating Kafka record at the same position.
///
/// **Coverage contract.** Together, [#succeededIndexes] and [#failedByIndex] must cover every
/// position in the input batch (i.e. their disjoint union must be exactly `[0, batchSize)`). The
/// `BatchPipelineWrapper` in `kpipe-consumer` enforces this — any indexes left unaccounted for
/// are treated as failures and routed to the configured DLQ. Implementations should not return
/// overlapping entries (an index appearing in both lists); behavior in that case is undefined.
///
/// **Sentinel factories.** Use [#allSucceeded] and [#allFailed] for the trivial all-or-nothing
/// outcomes — these are the natural results when a downstream system either confirms the whole
/// batch or rejects it wholesale (e.g. a transactional commit that succeeded or aborted).
///
/// @param succeededIndexes immutable list of input-batch indexes the sink confirmed as written
/// @param failedByIndex immutable map from input-batch index to the failure that occurred
public record BatchResult(List<Integer> succeededIndexes, Map<Integer, Exception> failedByIndex) {
  /// Canonical constructor — null-checks both arguments and defensively copies them so callers
  /// can pass mutable collections without aliasing the wrapper's internal view.
  public BatchResult {
    Objects.requireNonNull(succeededIndexes, "succeededIndexes cannot be null");
    Objects.requireNonNull(failedByIndex, "failedByIndex cannot be null");
    succeededIndexes = List.copyOf(succeededIndexes);
    failedByIndex = Map.copyOf(failedByIndex);
  }

  /// Convenience factory for "every record in the batch succeeded." Returns a `BatchResult` with
  /// `succeededIndexes = [0, batchSize)` and an empty failure map.
  ///
  /// @param batchSize the size of the input batch (must be ≥ 0)
  /// @return an all-succeeded result of the given size
  /// @throws IllegalArgumentException if `batchSize` is negative
  public static BatchResult allSucceeded(final int batchSize) {
    if (batchSize < 0) throw new IllegalArgumentException("batchSize cannot be negative, got " + batchSize);
    final var indexes = new ArrayList<Integer>(batchSize);
    for (int i = 0; i < batchSize; i++) indexes.add(i);
    return new BatchResult(indexes, Map.of());
  }

  /// Convenience factory for "every record in the batch failed with the same cause." Returns a
  /// `BatchResult` with an empty success list and `failedByIndex = {0: cause, 1: cause, ...}`.
  ///
  /// Use this when a sink wants to opt back into v1's whole-batch failure semantics for a
  /// specific batch (e.g. a transaction aborted and the granular failure information is not
  /// available to attribute per-record).
  ///
  /// @param batchSize the size of the input batch (must be ≥ 0)
  /// @param cause the exception to attribute to every record (must not be null)
  /// @return an all-failed result of the given size
  /// @throws IllegalArgumentException if `batchSize` is negative
  /// @throws NullPointerException if `cause` is null
  public static BatchResult allFailed(final int batchSize, final Exception cause) {
    if (batchSize < 0) throw new IllegalArgumentException("batchSize cannot be negative, got " + batchSize);
    Objects.requireNonNull(cause, "cause cannot be null");
    final var failures = new HashMap<Integer, Exception>(batchSize);
    for (int i = 0; i < batchSize; i++) failures.put(i, cause);
    return new BatchResult(List.of(), failures);
  }
}
