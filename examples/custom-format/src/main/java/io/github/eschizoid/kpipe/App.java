package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.config.AppConfig;
import io.github.eschizoid.kpipe.consumer.config.KafkaConsumerConfig;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;

/// Example consumer demonstrating the `KPipe.custom(topic, props, format)` entry point with a
/// user-supplied [MessageFormat]. The format here is a small CSV-line codec for `CsvOrder` records
/// implemented inline — no `kpipe-format-*` module required, no schema registry, no extra deps.
///
/// The point of this example is to show what a custom format owes the pipeline and what it does
/// not. The [MessageFormat] contract is just two methods:
///
/// ```java
/// byte[] serialize(T data);
/// T      deserialize(byte[] data);
/// ```
///
/// Everything else — wire-format prefixes, schema lookup, magic bytes — stays inside the format
/// implementation. The consumer entry point operates on `byte[]`; format SerDe lives in the
/// pipeline.
///
/// CSV wire shape (one record per Kafka message): `id,sku,quantity`. Whitespace around fields is
/// trimmed. Malformed lines throw — the pipeline routes them through the standard error path
/// rather than swallowing them as filtered.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());
    final var csvFormat = new CsvOrderFormat();

    final MessageSink<CsvOrder> sink = order ->
      LOGGER.log(Level.INFO, "received order id={0} sku={1} qty={2}", order.id(), order.sku(), order.quantity());

    try (
      final var handle = KPipe.custom(config.topic(), props, csvFormat)
        .filter(order -> order.quantity() > 0)
        .pipe(order -> new CsvOrder(order.id(), order.sku().toUpperCase(), order.quantity()))
        .toCustom(sink)
        .start()
    ) {
      LOGGER.log(Level.INFO, "Custom-format consumer started for topic {0}", config.topic());
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in custom-format consumer", e);
      System.exit(1);
    }
  }

  /// Domain record carried through the pipeline. Plain Java record — no annotations, no schema.
  ///
  /// @param id the order id
  /// @param sku the stock-keeping unit
  /// @param quantity the ordered quantity
  public record CsvOrder(String id, String sku, int quantity) {}

  /// Inline CSV-line codec implementing [MessageFormat] for [CsvOrder].
  ///
  /// Wire shape: UTF-8 bytes of `id,sku,quantity\n`. The trailing newline is optional on read.
  /// Malformed input throws `IllegalArgumentException` rather than returning null — per the KPipe
  /// pipeline contract, null deserialization is treated as a retryable failure and would mask
  /// genuine decode errors as filtered records. Throwing lets the pipeline surface them through
  /// the configured error handler / DLQ.
  public static final class CsvOrderFormat implements MessageFormat<CsvOrder> {

    @Override
    public byte[] serialize(final CsvOrder data) {
      if (data == null) return null;
      final var line = data.id() + "," + data.sku() + "," + data.quantity();
      return line.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public CsvOrder deserialize(final byte[] data) {
      if (data == null || data.length == 0) {
        throw new IllegalArgumentException("CSV payload is empty");
      }
      final var line = new String(data, StandardCharsets.UTF_8).strip();
      final var parts = line.split(",", -1);
      if (parts.length != 3) {
        throw new IllegalArgumentException("Expected 3 CSV fields (id,sku,quantity), got " + parts.length + ": " + line);
      }
      try {
        return new CsvOrder(parts[0].strip(), parts[1].strip(), Integer.parseInt(parts[2].strip()));
      } catch (final NumberFormatException e) {
        throw new IllegalArgumentException("Quantity is not an integer in line: " + line, e);
      }
    }
  }
}
