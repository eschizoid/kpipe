package io.github.eschizoid.kpipe.test;

import io.github.eschizoid.kpipe.record.RecordContext;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// A [RecordContext] that reads a [ConsumerRecord] in place, the way the consumer presents a
/// record it consumed. It mirrors the consumer module's own adapter, `KafkaRecordContext`, which is
/// not exported; a change to one belongs in both.
final class ConsumedRecordContext extends RecordContext {

  private final ConsumerRecord<byte[], byte[]> record;

  ConsumedRecordContext(final ConsumerRecord<byte[], byte[]> record) {
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
