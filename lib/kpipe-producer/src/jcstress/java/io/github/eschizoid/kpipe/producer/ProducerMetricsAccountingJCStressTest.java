package io.github.eschizoid.kpipe.producer;

import io.github.eschizoid.kpipe.metrics.ProducerMetrics;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.J_Result;

/// Concurrency-stress check that the producer's send-path metric accounting loses no increment
/// when sends run on separate threads at the same time.
///
/// The real [KPipeProducer] holds no counter of its own — every send outcome is reported to the
/// injected [ProducerMetrics]. So this drives the real producer over a [MockProducer] in
/// auto-complete mode (each send finishes immediately on the calling thread) with a recording
/// metrics implementation that increments an [AtomicLong] per success. Two actors each send one
/// record concurrently; both sends succeed, so the recorded sent count must total exactly two no
/// matter how the two calls interleave.
///
/// The arbiter reads the recorded total after both actors complete. The only acceptable outcome
/// is 2; a value of 1 would mean one success increment was lost to a read-modify-write race,
/// and 0 would mean both were lost.
@JCStressTest
@Outcome(id = "2", expect = Expect.ACCEPTABLE, desc = "Both concurrent sends recorded; no lost increment.")
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "A send-success increment was lost under concurrency.")
@State
public class ProducerMetricsAccountingJCStressTest {

  private static final String TOPIC = "jcstress-topic";

  private final AtomicLong sent = new AtomicLong();
  private final KPipeProducer<byte[], byte[]> producer;

  public ProducerMetricsAccountingJCStressTest() {
    final var mock = new MockProducer<byte[], byte[]>(
      true,
      (Partitioner) null,
      new ByteArraySerializer(),
      new ByteArraySerializer()
    );
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
    producer = KPipeProducer.<byte[], byte[]>builder().withProducer(mock).withMetrics(recording).build();
  }

  @Actor
  public void senderA() {
    producer.send(new ProducerRecord<>(TOPIC, "a".getBytes()));
  }

  @Actor
  public void senderB() {
    producer.send(new ProducerRecord<>(TOPIC, "b".getBytes()));
  }

  @Arbiter
  public void observe(final J_Result r) {
    r.r1 = sent.get();
  }
}
