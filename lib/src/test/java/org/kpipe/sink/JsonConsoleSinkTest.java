package org.kpipe.sink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JsonConsoleSinkTest {

  private JsonConsoleSink<Object, Object> sink;

  @Mock
  private Logger mockLogger;

  @Captor
  private ArgumentCaptor<String> messageCaptor;

  @BeforeEach
  void setUp() {
    // Remove the global stubbing from here and move to individual tests
    sink = new JsonConsoleSink<>(mockLogger, Level.INFO);
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
  void shouldFormatJsonByteArray() {
    // Arrange
    when(mockLogger.isLoggable(any(Level.class))).thenReturn(true);
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", new byte[0]);
    final var jsonString = """
            {
              "name": "test",
              "value": 123
            }""";
    final var jsonBytes = jsonString.getBytes(StandardCharsets.UTF_8);
    final var expectedJson =
      """
            {"topic":"test-topic","partition":0,"offset":123,"key":"test-key","processedMessage":"{\\n  \\"name\\": \\"test\\",\\n  \\"value\\": 123\\n}"}""";

    // Act
    sink.send(record, jsonBytes);

    // Assert
    verify(mockLogger).log(eq(Level.INFO), messageCaptor.capture());
    final var logMessage = messageCaptor.getValue();

    assertEquals(expectedJson, logMessage);
  }

  @Test
  void shouldHandleNonJsonByteArray() {
    // Arrange
    when(mockLogger.isLoggable(any(Level.class))).thenReturn(true);
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", new byte[0]);
    final var textBytes = "Hello, world!".getBytes(StandardCharsets.UTF_8);

    // Act
    sink.send(record, textBytes);

    // Assert
    verify(mockLogger).log(eq(Level.INFO), messageCaptor.capture());
    final var logMessage = messageCaptor.getValue();
    assertTrue(logMessage.contains("processedMessage"));
    assertTrue(logMessage.contains("Hello, world!"));
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
    assertTrue(messageCaptor.getValue().contains("processedMessage"));
    assertTrue(messageCaptor.getValue().contains("empty"));
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
    final var customSink = new JsonConsoleSink<>(mockLogger, Level.WARNING);
    final var record = new ConsumerRecord<Object, Object>("test-topic", 0, 123L, "test-key", "value");

    // Act
    customSink.send(record, "test");

    // Assert
    verify(mockLogger).log(eq(Level.WARNING), anyString());
    verify(mockLogger, never()).log(eq(Level.INFO), anyString());
  }
}
