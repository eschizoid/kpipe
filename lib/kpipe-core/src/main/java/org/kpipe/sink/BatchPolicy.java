package org.kpipe.sink;

import java.time.Duration;
import java.util.Objects;

/// Configuration for a [BatchSink] flush trigger. A flush fires whenever **either** threshold is
/// reached — `maxSize` records buffered, or `maxAge` since the oldest buffered record.
///
/// @param maxSize maximum number of records in a single batch (must be ≥ 1)
/// @param maxAge maximum time the oldest buffered record may sit before flush (must be positive)
public record BatchPolicy(int maxSize, Duration maxAge) {
  /// Canonical constructor — validates that `maxSize` is at least 1 and `maxAge` is strictly
  /// positive. Both rules guard against silently-broken configurations (a flush-of-zero or a
  /// past-due age threshold).
  public BatchPolicy {
    if (maxSize < 1) throw new IllegalArgumentException("maxSize must be >= 1, got " + maxSize);
    Objects.requireNonNull(maxAge, "maxAge cannot be null");
    if (maxAge.isNegative() || maxAge.isZero()) throw new IllegalArgumentException(
      "maxAge must be positive, got " + maxAge
    );
  }

  /// Convenience factory for size-driven batches with a permissive 1-minute age cap. Useful when
  /// throughput is high enough that the size trigger always fires first.
  ///
  /// @param maxSize maximum batch size
  /// @return a policy with the given size and a 1-minute age cap
  public static BatchPolicy ofSize(final int maxSize) {
    return new BatchPolicy(maxSize, Duration.ofMinutes(1));
  }

  /// Convenience factory for age-driven batches with a generous 10,000-record size cap. Useful
  /// when steady-state traffic is bursty and you want predictable flush latency.
  ///
  /// @param maxAge maximum buffered-record age
  /// @return a policy with the given age and a 10,000-record size cap
  public static BatchPolicy ofAge(final Duration maxAge) {
    return new BatchPolicy(10_000, maxAge);
  }
}
