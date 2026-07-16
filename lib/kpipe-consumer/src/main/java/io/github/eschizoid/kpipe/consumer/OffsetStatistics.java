package io.github.eschizoid.kpipe.consumer;

import java.util.Map;
import org.apache.kafka.common.TopicPartition;

/// Typed snapshot of an offset manager's overall state, returned by [OffsetManager#getStatistics].
/// Replaces the former stringly-typed `Map<String, Object>` so callers read fields directly instead
/// of casting magic-string keys. Test/observability use only — not on the user-facing `Handle`.
///
/// @param partitionCount                      distinct partitions being tracked
/// @param totalPendingOffsets                 in-flight offsets summed across all partitions
/// @param totalProcessedPartitions            partitions with at least one processed offset
/// @param managerState                        the offset manager's lifecycle state
/// @param highestProcessedOffsetsByPartition  per-partition highest processed offset (empty if none)
/// @param averageHighestProcessedOffset       mean of the per-partition highest offsets, `0` if none
/// @param pendingCommits                      commits issued but not yet acknowledged
public record OffsetStatistics(
  int partitionCount,
  int totalPendingOffsets,
  int totalProcessedPartitions,
  OffsetState managerState,
  Map<TopicPartition, Long> highestProcessedOffsetsByPartition,
  double averageHighestProcessedOffset,
  int pendingCommits
) {
  /// An empty statistics snapshot for offset managers that don't track per-partition detail
  /// (e.g. test fakes and external-store managers).
  ///
  /// @return a zeroed snapshot in the `CREATED` state
  public static OffsetStatistics empty() {
    return new OffsetStatistics(0, 0, 0, OffsetState.CREATED, Map.of(), 0.0, 0);
  }
}
