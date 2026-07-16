package io.github.eschizoid.kpipe.consumer;

import java.util.OptionalLong;

/// Typed snapshot of one partition's offset-tracking state, returned by
/// [KafkaOffsetManager#getPartitionState]. Replaces the former stringly-typed `Map<String, Object>`
/// so callers read fields directly instead of casting magic-string keys.
///
/// @param nextOffsetToCommit    the offset the manager would next commit, or `-1` if the partition
///                              has neither pending nor processed offsets
/// @param highestProcessedOffset the highest offset marked processed, or `-1` if none
/// @param managerState          the offset manager's lifecycle state
/// @param pendingCount          number of offsets currently in flight (not yet committed)
/// @param lowestPendingOffset   the lowest in-flight offset, empty if none pending
/// @param highestPendingOffset  the highest in-flight offset, empty if none pending
public record PartitionState(
  long nextOffsetToCommit,
  long highestProcessedOffset,
  OffsetState managerState,
  int pendingCount,
  OptionalLong lowestPendingOffset,
  OptionalLong highestPendingOffset
) {}
