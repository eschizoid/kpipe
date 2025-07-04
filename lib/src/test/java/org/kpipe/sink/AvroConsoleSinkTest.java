package org.kpipe.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AvroConsoleSinkTest {

  private AvroConsoleSink<Object, Object> sink;

  @Mock
  private Logger mockLogger;

  @Captor
  private ArgumentCaptor<String> messageCaptor;

  @BeforeEach
  void setUp() {
    sink = new AvroConsoleSink<>(mockLogger, Level.INFO);
  }

  @Test
  void shouldLogMessageAsJson() {
    // Arrange
    when(mockLogger.isLoggable(any(Level.class))).thenReturn(true);
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", "original-value");

    // Act
    sink.send(record, "processed-value");

    // Assert
    verify(mockLogger).log(eq(Level.INFO), messageCaptor.capture());
    final var logMessage = messageCaptor.getValue();
    final var expectedJson =
      """
                  {"topic":"test-topic","partition":0,"offset":123,"key":"test-key","processedMessage":"processed-value"}""";
    assertEquals(expectedJson, logMessage);
  }

  @Test
  void shouldHandleNullValue() {
    // Arrange
    when(mockLogger.isLoggable(any(Level.class))).thenReturn(true);
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", "original-value");

    // Act
    sink.send(record, null);

    // Assert
    verify(mockLogger).log(eq(Level.INFO), messageCaptor.capture());
    assertTrue(messageCaptor.getValue().contains("\"processedMessage\":\"null\""));
  }

  @Test
  void shouldHandleEmptyByteArray() {
    // Arrange
    when(mockLogger.isLoggable(any(Level.class))).thenReturn(true);
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", new byte[0]);

    // Act
    sink.send(record, new byte[0]);

    // Assert
    verify(mockLogger).log(eq(Level.INFO), messageCaptor.capture());
    assertTrue(messageCaptor.getValue().contains("\"processedMessage\":\"empty\""));
  }

  @Test
  void shouldFormatAvroByteArray() throws IOException {
    // Arrange
    when(mockLogger.isLoggable(any(Level.class))).thenReturn(true);
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", new byte[0]);

    // Create avro data
    final var avroBytes = createSampleAvroRecord();

    // Act
    sink.send(record, avroBytes);

    // Assert
    verify(mockLogger).log(eq(Level.INFO), messageCaptor.capture());
    final var logMessage = messageCaptor.getValue();

    assertTrue(logMessage.contains("\"topic\":\"test-topic\""));
    assertTrue(logMessage.contains("\"key\":\"test-key\""));
    assertTrue(logMessage.contains("\"processedMessage\":"));
  }

  @Test
  void shouldHandleInvalidAvroData() {
    // Arrange
    when(mockLogger.isLoggable(any(Level.class))).thenReturn(true);
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", new byte[0]);
    final var invalidAvroBytes = "Not valid Avro".getBytes(StandardCharsets.UTF_8);

    // Act
    sink.send(record, invalidAvroBytes);

    // Assert
    verify(mockLogger).log(eq(Level.INFO), messageCaptor.capture());
    final var logMessage = messageCaptor.getValue();

    assertTrue(logMessage.contains("\"topic\":\"test-topic\""));
    assertTrue(logMessage.contains("\"key\":\"test-key\""));
    assertTrue(logMessage.contains("\"processedMessage\":\"\""));
  }

  @Test
  void shouldNotLogWhenLevelIsNotLoggable() {
    // Arrange
    when(mockLogger.isLoggable(any(Level.class))).thenReturn(false);
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", "value");

    // Act
    sink.send(record, "test");

    // Assert
    verify(mockLogger, never()).log(eq(Level.INFO), anyString());
  }

  @Test
  void shouldUseCustomLogLevel() {
    // Arrange
    when(mockLogger.isLoggable(any(Level.class))).thenReturn(true);
    final var customSink = new AvroConsoleSink<>(mockLogger, Level.WARNING);
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", "value");

    // Act
    customSink.send(record, "test");

    // Assert
    verify(mockLogger).log(eq(Level.WARNING), anyString());
    verify(mockLogger, never()).log(eq(Level.INFO), anyString());
  }

  @Test
  void shouldHandleExceptionsGracefully() {
    // Arrange
    when(mockLogger.isLoggable(any(Level.class))).thenReturn(true);
    doThrow(new RuntimeException("Test exception")).when(mockLogger).log(eq(Level.INFO), anyString());
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", "value");

    // Act - should not throw exception
    sink.send(record, "test");

    // Assert
    verify(mockLogger).log(eq(Level.ERROR), anyString(), any(Exception.class));
  }

  private byte[] createSampleAvroRecord() throws IOException {
    final var schemaJson =
      """
        {
          "type": "record",
          "name": "TestRecord",
          "fields":[
            {"name": "field1", "type": "string"},
            {"name": "field2", "type": "int"}
          ]
        }""";
    final var schema = new Schema.Parser().parse(schemaJson);

    final var record = new GenericData.Record(schema);
    record.put("field1", "test value");
    record.put("field2", 42);

    final var outputStream = new ByteArrayOutputStream();
    final var writer = new GenericDatumWriter<GenericRecord>(schema);
    final var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    writer.write(record, encoder);
    encoder.flush();

    return outputStream.toByteArray();
  }
}
