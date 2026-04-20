package org.kpipe.consumer.sink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.kpipe.processor.AvroMessageProcessor;
import org.kpipe.sink.MessageSink;

/// A sink that logs processed messages with Avro formatting.
///
/// @param <T> The type of message to log
/// @param schema The Avro schema used to decode byte array messages
public record AvroConsoleSink<T>(Schema schema) implements MessageSink<T> {
  private static final Logger LOGGER = System.getLogger(AvroConsoleSink.class.getName());

  /// Creates an `AvroConsoleSink` using the default schema version "1".
  public AvroConsoleSink() {
    this(AvroMessageProcessor.getSchema("1"));
  }

  @Override
  public void accept(final T processedValue) {
    ConsoleSinkSupport.log(LOGGER, processedValue, this::formatValue);
  }

  private String formatValue(final T value) {
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
