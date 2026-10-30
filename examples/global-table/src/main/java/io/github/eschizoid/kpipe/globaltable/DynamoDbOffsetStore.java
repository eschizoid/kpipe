package io.github.eschizoid.kpipe.globaltable;

import io.github.eschizoid.kpipe.consumer.OffsetStore;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.TopicPartition;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/// An [OffsetStore] backed by a DynamoDB table, for a consumer group that fails over between
/// regions.
///
/// With a global table, each region's consumers read and write their local replica. When a region
/// goes down, a consumer in another region loads the last commit frontier that replicated there and
/// resumes from it. Replication is asynchronous, so that frontier can be older than the last one
/// the failed region wrote, and the records in between are processed again. None is skipped: the
/// store only ever holds a frontier, and every offset below a frontier finished processing.
///
/// An offset means the same record in every region only when the topic is replicated with its
/// offsets preserved.
///
/// Each item holds one partition's next offset to read in [#OFFSET], keyed in [#KEY] by group,
/// topic and partition. Each partition is written separately; a commit that fails partway leaves
/// every partition at a frontier it really reached, and the next commit carries the rest.
///
/// The store does not fence writers. If the failed region is still running, both regions write,
/// and the last writer wins, which can move a partition's offset backwards: more reprocessing,
/// never a skip.
public final class DynamoDbOffsetStore implements OffsetStore {

  /// The partition key attribute: group, topic and partition.
  public static final String KEY = "pk";

  /// The next offset to read, as a number.
  public static final String OFFSET = "next_offset";

  private final DynamoDbClient client;
  private final String table;
  private final String group;

  /// @param client the client for this region's replica of the table
  /// @param table the table name, with [#KEY] as its string partition key
  /// @param group the consumer group whose offsets this store keeps
  public DynamoDbOffsetStore(final DynamoDbClient client, final String table, final String group) {
    this.client = Objects.requireNonNull(client, "client");
    this.table = Objects.requireNonNull(table, "table");
    this.group = Objects.requireNonNull(group, "group");
  }

  @Override
  public Map<TopicPartition, Long> load(final Collection<TopicPartition> partitions) {
    final var offsets = new HashMap<TopicPartition, Long>();
    for (final var partition : partitions) {
      final var item = client
        .getItem(request -> request.tableName(table).key(key(partition)).consistentRead(true))
        .item();
      if (item.containsKey(OFFSET)) offsets.put(partition, Long.parseLong(item.get(OFFSET).n()));
    }
    return offsets;
  }

  @Override
  public void commit(final Map<TopicPartition, Long> offsets) {
    offsets.forEach((partition, offset) -> {
      final var item = new HashMap<>(key(partition));
      item.put(OFFSET, AttributeValue.fromN(Long.toString(offset)));
      client.putItem(request -> request.tableName(table).item(item));
    });
  }

  private Map<String, AttributeValue> key(final TopicPartition partition) {
    return Map.of(KEY, AttributeValue.fromS(group + "|" + partition.topic() + "|" + partition.partition()));
  }
}
