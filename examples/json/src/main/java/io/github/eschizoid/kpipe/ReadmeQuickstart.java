package io.github.eschizoid.kpipe;

import java.util.Properties;

/// The root README's first example, kept as compiled code so the snippet cannot drift from the
/// API. If this class stops compiling, the README example is broken — fix both together.
public final class ReadmeQuickstart {

  private ReadmeQuickstart() {}

  static void main() throws InterruptedException {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "orders-service");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("enable.auto.commit", "false"); // KPipe manages offset commits itself
    props.put("auto.offset.reset", "earliest");

    try (final var handle = KPipe.json("orders", props)
        .pipe(order -> {
          order.put("processedAt", System.currentTimeMillis());
          return order;
        })
        .filter(order -> order.get("customerId") != null)
        .toConsole()
        .start()) {
      handle.awaitShutdown(); // blocks until close() or a JVM shutdown signal
    }
  }
}
