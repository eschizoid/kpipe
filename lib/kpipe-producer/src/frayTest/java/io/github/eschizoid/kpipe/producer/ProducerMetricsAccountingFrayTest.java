package io.github.eschizoid.kpipe.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.eschizoid.kpipe.metrics.ProducerMetrics;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.FrayTest;

/// Fray port of the producer send-accounting race.
///
/// Two sends complete concurrently and each must record exactly one success. The counter is what
/// operators read to tell "the pipeline is delivering" from "the pipeline is silently dropping",
/// so an increment lost to a race makes the metric under-report real throughput and, worse, makes
/// a genuine delivery gap indistinguishable from a counting bug.
@ExtendWith(FrayTestExtension.class)
@Tag("FrayTest")
class ProducerMetricsAccountingFrayTest {

  private static final String TOPIC = "fray-topic";

  @FrayTest(iterations = 500)
  void concurrentSendsAreBothCounted() {
    final var sent = new AtomicLong();
    final var mock = new MockProducer<byte[], byte[]>(
      true,
      (Partitioner) null,
      new ByteArraySerializer(),
      new ByteArraySerializer()
    );
    // Counting fake: only the success path is instrumented, since that is the increment at stake.
    final var recording = new ProducerMetrics() {
      @Override
      public void recordMessageSent() {
        sent.incrementAndGet();
      }

      @Override
      public void recordMessageFailed() {}

      @Override
      public void recordDlqSent() {}

      @Override
      public void recordDlqFailed() {}
    };
    final var producer = KPipeProducer.<byte[], byte[]>builder().withProducer(mock).withMetrics(recording).build();

    FrayScenariosProducer.runConcurrently(
      () -> producer.send(new ProducerRecord<>(TOPIC, "a".getBytes())),
      () -> producer.send(new ProducerRecord<>(TOPIC, "b".getBytes()))
    );

    assertEquals(2L, sent.get(), "a send-success increment was lost under concurrency");
  }
}
