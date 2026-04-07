package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.processor.AvroMessageProcessor;
import org.kpipe.processor.JsonMessageProcessor;

class OptimizedPipelineTest {

  private MessageProcessorRegistry registry;
  private static final String SOURCE_APP = "test-app";

  @BeforeEach
  void setUp() {
    registry = new MessageProcessorRegistry(SOURCE_APP);
    AvroMessageProcessor.clearSchemaRegistry();
  }

  @Test
  void testOptimizedJsonPipeline() {
    // Register operators
    final var addSource = RegistryKey.json("addSource");
    final var addStatus = RegistryKey.json("addStatus");

    registry.register(addSource, JsonMessageProcessor.addFieldOperator("source", SOURCE_APP));
    registry.register(addStatus, JsonMessageProcessor.addFieldOperator("status", "processed"));

    // Create optimized pipeline
    final var pipeline = registry.pipeline(MessageFormat.JSON).add(addSource, addStatus).build();

    // Process message
    final var input = "{\"id\":\"123\"}".getBytes(StandardCharsets.UTF_8);
    final var output = pipeline.apply(input);

    final var result = new String(output, StandardCharsets.UTF_8);
    assertTrue(result.contains("\"source\":\"test-app\""));
    assertTrue(result.contains("\"status\":\"processed\""));
    assertTrue(result.contains("\"id\":\"123\""));
  }

  @Test
  void testOptimizedAvroPipeline() throws java.io.IOException {
    final var schemaJson = """
      {
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "source", "type": ["null", "string"], "default": null},
          {"name": "status", "type": ["null", "string"], "default": null}
        ]
      }
      """;
    AvroMessageProcessor.registerSchema("user", schemaJson);
    final var schema = AvroMessageProcessor.getSchema("user");

    // Register operators
    final var addSource = RegistryKey.avro("addSource");
    final var addStatus = RegistryKey.avro("addStatus");

    registry.register(addSource, AvroMessageProcessor.addFieldOperator("source", SOURCE_APP));
    registry.register(addStatus, AvroMessageProcessor.addFieldOperator("status", "processed"));

    // Create optimized pipeline
    final var format = ((AvroFormat) MessageFormat.AVRO).withDefaultSchema("user");
    final var pipeline = registry.pipeline(format).add(addSource, addStatus).build();

    // Create input record
    final var record = new GenericData.Record(schema);
    record.put("id", "123");

    // Serialize record
    final var out = new ByteArrayOutputStream();
    final var encoder = EncoderFactory.get().binaryEncoder(out, null);
    final var writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    final var input = out.toByteArray();

    // Process message
    final var output = pipeline.apply(input);

    // Verify
    assertNotNull(output);
    final var decoded = format.deserialize(output);
    assertEquals("123", decoded.get("id").toString());
    assertEquals(SOURCE_APP, decoded.get("source").toString());
    assertEquals("processed", decoded.get("status").toString());
  }

  @Test
  void testOptimizedJsonPipelineDefaultOperators() {
    // Create optimized pipeline using default operators
    final var pipeline = registry
      .pipeline(MessageFormat.JSON)
      .add(
        MessageProcessorRegistry.JSON_ADD_SOURCE,
        MessageProcessorRegistry.JSON_ADD_TIMESTAMP,
        MessageProcessorRegistry.JSON_MARK_PROCESSED
      )
      .build();

    // Process message
    final var input = "{\"id\":\"123\"}".getBytes(StandardCharsets.UTF_8);
    final var output = pipeline.apply(input);

    final var result = new String(output, StandardCharsets.UTF_8);
    assertTrue(result.contains("\"source\":\"test-app\""));
    assertTrue(result.contains("\"processed\":\"true\""));
    assertTrue(result.contains("\"timestamp\":"));
    assertTrue(result.contains("\"id\":\"123\""));
  }

  @Test
  void testOptimizedAvroPipelineDefaultOperators() throws java.io.IOException {
    final var schemaJson = """
      {
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "source", "type": ["null", "string"], "default": null},
          {"name": "processed", "type": ["null", "string"], "default": null},
          {"name": "timestamp", "type": ["null", "long"], "default": null}
        ]
      }
      """;
    registry = new MessageProcessorRegistry(SOURCE_APP, MessageFormat.AVRO);
    registry.addSchema("user", "com.example.User", schemaJson);
    final var schema = AvroMessageProcessor.getSchema("user");

    // Create optimized pipeline using default operators
    // Note: Default operators are registered for the whole registry if format is JSON,
    // but here we are using AVRO. We need to register them for AVRO specifically if we want to use
    // them.
    registry.register(RegistryKey.avro("addSource"), AvroMessageProcessor.addFieldOperator("source", SOURCE_APP));
    registry.register(RegistryKey.avro("markProcessed"), AvroMessageProcessor.addFieldOperator("processed", "true"));
    registry.register(RegistryKey.avro("addTimestamp"), AvroMessageProcessor.addTimestampOperator("timestamp"));

    final var format = ((AvroFormat) MessageFormat.AVRO).withDefaultSchema("user");
    final var pipeline = registry
      .pipeline(format)
      .add(RegistryKey.avro("addSource"), RegistryKey.avro("markProcessed"), RegistryKey.avro("addTimestamp"))
      .build();

    // Create input record
    final var record = new GenericData.Record(schema);
    record.put("id", "123");

    // Serialize record
    final var out = new ByteArrayOutputStream();
    final var encoder = EncoderFactory.get().binaryEncoder(out, null);
    final var writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    final var input = out.toByteArray();

    // Process message
    final var output = pipeline.apply(input);

    // Verify
    assertNotNull(output);
    final var decoded = format.deserialize(output);
    assertEquals("123", decoded.get("id").toString());
    assertEquals(SOURCE_APP, decoded.get("source").toString());
    assertEquals("true", decoded.get("processed").toString());
    assertNotNull(decoded.get("timestamp"));
  }

  @Test
  void testOptimizedAvroPipelineWithOffset() throws java.io.IOException {
    final var schemaJson = """
      {
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "source", "type": ["null", "string"], "default": null}
        ]
      }
      """;
    AvroMessageProcessor.registerSchema("userOffset", schemaJson);
    final var schema = AvroMessageProcessor.getSchema("userOffset");

    // Register operators
    final var addSource = RegistryKey.avro("addSource");
    registry.register(addSource, AvroMessageProcessor.addFieldOperator("source", SOURCE_APP));

    // Create optimized pipeline with offset 5 (simulating magic bytes)
    final var format = ((AvroFormat) MessageFormat.AVRO).withDefaultSchema("userOffset");
    final var pipeline = registry.pipeline(format).skipBytes(5).add(addSource).build();

    // Create input record
    final var record = new GenericData.Record(schema);
    record.put("id", "123");

    // Serialize record
    final var out = new ByteArrayOutputStream();
    // Add 5 magic bytes
    out.write(new byte[] { 0, 0, 0, 0, 1 });
    final var encoder = EncoderFactory.get().binaryEncoder(out, null);
    final var writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    final var input = out.toByteArray();

    // Process message
    final var output = pipeline.apply(input);

    // Verify
    assertNotNull(output);
    final var decoded = format.deserialize(output);
    assertEquals("123", decoded.get("id").toString());
    assertEquals(SOURCE_APP, decoded.get("source").toString());
  }
}
