package org.kpipe.sink;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import org.apache.avro.Schema;
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
 * @param schema The Avro schema used to decode byte array messages
 */
public record AvroConsoleSink<K, V>(Schema schema) implements MessageSink<K, V> {
  private static final DslJson<Object> DSL_JSON = new DslJson<>();
  private static final Logger LOGGER = System.getLogger(AvroConsoleSink.class.getName());
  private static final Level LOG_LEVEL = Level.INFO;

  /** Creates an {@code AvroConsoleSink} using the default schema version "1". */
  public AvroConsoleSink() {
    this(AvroMessageProcessor.getSchema("1"));
  }

  @Override
  public void send(final ConsumerRecord<K, V> record, final V processedValue) {
    try {
      if (!LOGGER.isLoggable(LOG_LEVEL)) return;
      final var logData = LinkedHashMap.newLinkedHashMap(5);
      logData.put("topic", record.topic());
      logData.put("partition", record.partition());
      logData.put("offset", record.offset());
      logData.put("key", String.valueOf(record.key()));
      logData.put("processedMessage", formatValue(processedValue));

      try (final var out = new ByteArrayOutputStream()) {
        DSL_JSON.serialize(logData, out);
        LOGGER.log(LOG_LEVEL, out.toString(StandardCharsets.UTF_8));
      } catch (final IOException e) {
        LOGGER.log(
          Level.WARNING,
          "Failed to process message (topic=%s, partition=%d, offset=%d)".formatted(
              record.topic(),
              record.partition(),
              record.offset()
            )
        );
      }
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Error in AvroConsoleSink while processing message", e);
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
      final var writer = new GenericDatumWriter<GenericRecord>(schema);
      final var decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
      final var encoder = EncoderFactory.get().jsonEncoder(schema, outputStream);
      final var record = new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
      writer.write(record, encoder);
      encoder.flush();
      return outputStream.toString(StandardCharsets.UTF_8);
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Failed to parse Avro data", e);
      return "";
    }
  }
}
