package org.kpipe.sink;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kpipe.processor.AvroMessageProcessor;

/**
 * A sink that logs processed Kafka messages with Avro formatting.
 *
 * @param <K> The type of message key
 * @param <V> The type of message value
 */
public class AvroConsoleSink<K, V> implements MessageSink<K, V> {

  private static final DslJson<Object> DSL_JSON = new DslJson<>();
  private final Level logLevel;
  private final Logger logger;

  /**
   * Creates an AvroConsoleSink with the specified log level and Schema Registry URL.
   *
   * @param logger The logger to use for logging messages
   * @param logLevel The log level to use for logging messages
   */
  public AvroConsoleSink(final Logger logger, final Level logLevel) {
    this.logLevel = logLevel;
    this.logger = logger;
  }

  @Override
  public void send(final ConsumerRecord<K, V> record, final V processedValue) {
    try {
      if (!logger.isLoggable(logLevel)) return;
      final var logData = LinkedHashMap.newLinkedHashMap(5);
      logData.put("topic", record.topic());
      logData.put("partition", record.partition());
      logData.put("offset", record.offset());
      logData.put("key", String.valueOf(record.key()));
      logData.put("processedMessage", formatValue(processedValue));

      try (final var out = new ByteArrayOutputStream()) {
        DSL_JSON.serialize(logData, out);
        logger.log(logLevel, out.toString(StandardCharsets.UTF_8));
      } catch (final IOException e) {
        logger.log(
          Level.WARNING,
          "Failed to process message (topic=%s, partition=%d, offset=%d)".formatted(
              record.topic(),
              record.partition(),
              record.offset()
            )
        );
      }
    } catch (final Exception e) {
      logger.log(Level.ERROR, "Error in AvroConsoleSink while processing message", e);
    }
  }

  private String formatValue(final V value) {
    if (value == null) return "null";
    if (value instanceof byte[] bytes) {
      if (bytes.length == 0) return "empty";
      return formatAvroData(bytes);
    }
    return String.valueOf(value);
  }

  private String formatAvroData(final byte[] bytes) {
    try {
      final var inputStream = new ByteArrayInputStream(bytes);
      final var outputStream = new ByteArrayOutputStream();
      final var schema = AvroMessageProcessor.getSchema("1");
      final var writer = new GenericDatumWriter<GenericRecord>(schema);

      final var decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
      final var encoder = EncoderFactory.get().jsonEncoder(schema, outputStream);

      final var record = new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
      writer.write(record, encoder);
      encoder.flush();

      return outputStream.toString(StandardCharsets.UTF_8);
    } catch (final Exception e) {
      logger.log(Level.ERROR, "Failed to parse Avro data", e);
      return "";
    }
  }
}
