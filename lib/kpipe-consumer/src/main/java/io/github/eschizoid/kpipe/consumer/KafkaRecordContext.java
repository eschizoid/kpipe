package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.record.RecordContext;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// A [RecordContext] that reads a consumed record in place. It returns the record's own arrays and
/// reads headers only when asked. The test kit's `ConsumedRecordContext` mirrors it, so a test
/// sees records as the consumer presents them; a change to one belongs in both.
final class KafkaRecordContext extends RecordContext {

  private final ConsumerRecord<byte[], byte[]> record;

  KafkaRecordContext(final ConsumerRecord<byte[], byte[]> record) {
    this.record = record;
  }

  @Override
  public String topic() {
    return record.topic();
  }

  @Override
  public int partition() {
    return record.partition();
  }

  @Override
  public long offset() {
    return record.offset();
  }

  @Override
  public long timestamp() {
    return record.timestamp();
  }

  @Override
  public byte[] key() {
    return record.key();
  }

  @Override
  public byte[] originalKey() {
    return record.key();
  }

  @Override
  public Optional<byte[]> header(final String name) {
    final var header = record.headers().lastHeader(name);
    return header == null ? Optional.empty() : Optional.ofNullable(header.value());
  }

  @Override
  public void forEachHeader(final BiConsumer<String, byte[]> action) {
    for (final var header : record.headers()) action.accept(header.key(), header.value());
  }
}
