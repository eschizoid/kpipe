package io.github.eschizoid.kpipe.producer;

import static org.junit.jupiter.api.Assertions.*;

import io.github.eschizoid.kpipe.producer.tracing.Tracer;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/// Durability and configuration-passthrough coverage for [KPipeProducer], driven by a real
/// [MockProducer] rather than a Mockito stub.
///
/// The Mockito-based tests in [KPipeProducerTest] verify the return value against pre-resolved
/// `CompletableFuture` stubs. These tests use [MockProducer] in manual-completion mode so the
/// broker ack future is resolved (or errored) explicitly, exercising the same code path a live
/// broker drives: `producer.send(record).get()` blocks until the ack lands.
///
/// The load-bearing property: `sendToDlq` returns `true` only when the record's ack future
/// completes without error. The consumer commits the source offset solely on a `true` return;
/// a `true` on a non-durable send would silently lose the record on the source topic. A `false`
/// leaves the offset pending so the record is reprocessed.
class KPipeProducerDurabilityTest {

  private static final String TOPIC = "source-topic";
  private static final String DLQ_TOPIC = "source-topic-dlq";

  private static MockProducer<byte[], byte[]> manualMockProducer() {
    // autoComplete=false → send() returns an incomplete future; the test drives completeNext /
    // errorNext to resolve it, mirroring a broker that acks (or fails) asynchronously.
    return new MockProducer<>(false, (Partitioner) null, new ByteArraySerializer(), new ByteArraySerializer());
  }

  private static ConsumerRecord<byte[], byte[]> failedRecord() {
    return new ConsumerRecord<>(TOPIC, 0, 7L, "k".getBytes(), "v".getBytes());
  }

  /// A completed ack future must surface as `true`, and the record must reach the underlying
  /// producer. The result is undecided until the ack lands.
  @Test
  void sendToDlqReturnsTrueOnlyAfterDurableAck() throws Exception {
    final var mock = manualMockProducer();
    final var producer = new KPipeProducer<>(mock, false, null, Tracer.noop());

    final var done = new CountDownLatch(1);
    final var result = new boolean[1];
    final var thread = Thread.ofVirtual().start(() -> {
      result[0] = producer.sendToDlq(DLQ_TOPIC, failedRecord(), TOPIC, new RuntimeException("boom"));
      done.countDown();
    });

    // The send is parked on get(); the ack future is not yet complete, so the result is undecided.
    awaitBufferedSend(mock);
    assertEquals(1, done.getCount(), "sendToDlq must not return before the broker ack lands");

    assertTrue(mock.completeNext(), "MockProducer should have one buffered send to complete");
    assertTrue(done.await(5, TimeUnit.SECONDS), "sendToDlq must return once the ack completes");
    thread.join();

    assertTrue(result[0], "a completed ack must surface as true");
    assertEquals(1, mock.history().size(), "the record must have reached the underlying producer");
    assertEquals(DLQ_TOPIC, mock.history().getFirst().topic());
  }

  /// A broker rejection must surface as `false` so the caller leaves the source offset pending.
  @Test
  void sendToDlqReturnsFalseOnSendError() throws Exception {
    final var mock = manualMockProducer();
    final var producer = new KPipeProducer<>(mock, false, null, Tracer.noop());

    final var done = new CountDownLatch(1);
    final var result = new boolean[] { true };
    Thread.ofVirtual().start(() -> {
      result[0] = producer.sendToDlq(DLQ_TOPIC, failedRecord(), TOPIC, new RuntimeException("boom"));
      done.countDown();
    });

    awaitBufferedSend(mock);
    assertTrue(mock.errorNext(new TimeoutException("broker unreachable")), "buffered send should error");

    assertTrue(done.await(5, TimeUnit.SECONDS), "sendToDlq must return once the ack future errors");
    assertFalse(result[0], "a failed ack must surface as false so the offset is held");
  }

  /// A transient failure that the application retries by calling `sendToDlq` again must end in a
  /// `true` once the retry's ack lands. The first attempt errors; the second succeeds. This mirrors
  /// the consumer reprocessing a held offset after a transient DLQ outage.
  @Test
  void sendToDlqRetryAfterTransientFailureEventuallySucceeds() throws Exception {
    final var mock = manualMockProducer();
    final var producer = new KPipeProducer<>(mock, false, null, Tracer.noop());

    // Attempt 1: transient failure → false.
    final var firstDone = new CountDownLatch(1);
    final var firstResult = new boolean[] { true };
    Thread.ofVirtual().start(() -> {
      firstResult[0] = producer.sendToDlq(DLQ_TOPIC, failedRecord(), TOPIC, new RuntimeException("boom"));
      firstDone.countDown();
    });
    awaitBufferedSend(mock);
    assertTrue(mock.errorNext(new NetworkException("transient blip")), "first send should error");
    assertTrue(firstDone.await(5, TimeUnit.SECONDS));
    assertFalse(firstResult[0], "transient failure must surface as false");

    // Attempt 2: retry of the same record completes durably → true.
    final var secondDone = new CountDownLatch(1);
    final var secondResult = new boolean[1];
    Thread.ofVirtual().start(() -> {
      secondResult[0] = producer.sendToDlq(DLQ_TOPIC, failedRecord(), TOPIC, new RuntimeException("boom"));
      secondDone.countDown();
    });
    awaitBufferedSend(mock);
    assertTrue(mock.completeNext(), "retry send should complete");
    assertTrue(secondDone.await(5, TimeUnit.SECONDS));

    assertTrue(secondResult[0], "the durable retry must surface as true");
  }

  /// `send` (the synchronous path the DLQ uses) returns the broker-assigned metadata once the ack
  /// lands and rethrows as a wrapped failure when the ack errors — the two outcomes `sendToDlq`
  /// keys its boolean off of.
  @Test
  void synchronousSendSurfacesAckOutcome() throws Exception {
    final var mock = manualMockProducer();
    final var producer = new KPipeProducer<>(mock, false, null, Tracer.noop());

    final var done = new CountDownLatch(1);
    final var thrown = new Throwable[1];
    Thread.ofVirtual().start(() -> {
      try {
        producer.send(new ProducerRecord<>(TOPIC, "k".getBytes(), "v".getBytes()));
      } catch (final Throwable t) {
        thrown[0] = t;
      }
      done.countDown();
    });

    awaitBufferedSend(mock);
    assertTrue(mock.errorNext(new TimeoutException("no ack")), "buffered send should error");
    assertTrue(done.await(5, TimeUnit.SECONDS));

    assertNotNull(thrown[0], "an errored ack must propagate as a thrown failure");
    assertEquals("Send failed", thrown[0].getMessage());
  }

  /// The builder forwards reliability config (`enable.idempotence`, `acks`, in-flight cap, retries)
  /// untouched to the underlying Kafka producer properties. The wrapper only fills serializer and
  /// `client.id` defaults; it must never silently downgrade the durability guarantees a caller set.
  @Test
  void builderForwardsIdempotenceAndAcksConfigUntouched() {
    final var userProps = new Properties();
    userProps.setProperty("bootstrap.servers", "broker:9092");
    userProps.setProperty("enable.idempotence", "true");
    userProps.setProperty("acks", "all");
    userProps.setProperty("max.in.flight.requests.per.connection", "5");
    userProps.setProperty("retries", "2147483647");

    final var built = KPipeProducer.Builder.buildProducerProperties(userProps);

    assertEquals("true", built.getProperty("enable.idempotence"), "idempotence flag must pass through");
    assertEquals("all", built.getProperty("acks"), "acks must not be downgraded");
    assertEquals("5", built.getProperty("max.in.flight.requests.per.connection"));
    assertEquals("2147483647", built.getProperty("retries"));
  }

  /// The wrapper does not inject an `acks` or `enable.idempotence` default of its own — absence in
  /// means absence out, so the Kafka client's own defaults apply and the wrapper never masks them.
  @Test
  void builderDoesNotInjectReliabilityDefaults() {
    final var userProps = new Properties();
    userProps.setProperty("bootstrap.servers", "broker:9092");

    final var built = KPipeProducer.Builder.buildProducerProperties(userProps);

    assertNull(built.getProperty("acks"), "wrapper must not invent an acks default");
    assertNull(built.getProperty("enable.idempotence"), "wrapper must not invent an idempotence default");
  }

  /// Spin until the send is buffered in the MockProducer so the test can deterministically drive
  /// completeNext / errorNext. The sending virtual thread is parked on get() at that point.
  private static void awaitBufferedSend(final MockProducer<byte[], byte[]> mock) throws InterruptedException {
    final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (mock.flushed() && System.nanoTime() < deadline) {
      Thread.sleep(2);
    }
    assertFalse(mock.flushed(), "the send should have been buffered for completion");
  }
}
