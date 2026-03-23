package org.kpipe.registry;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
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

    registry.registerOperator(addSource, JsonMessageProcessor.addFieldOperator("source", SOURCE_APP));
    registry.registerOperator(addStatus, JsonMessageProcessor.addFieldOperator("status", "processed"));

    // Create optimized pipeline
    final var pipeline = registry.jsonPipelineBuilder().add(addSource).add(addStatus).build();

    // Process message
    byte[] input = "{\"id\":\"123\"}".getBytes(StandardCharsets.UTF_8);
    byte[] output = pipeline.apply(input);

    String result = new String(output, StandardCharsets.UTF_8);
    assertTrue(result.contains("\"source\":\"test-app\""));
    assertTrue(result.contains("\"status\":\"processed\""));
    assertTrue(result.contains("\"id\":\"123\""));
  }

  @Test
  void testOptimizedAvroPipeline() throws java.io.IOException {
    String schemaJson =
      """
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
    Schema schema = AvroMessageProcessor.getSchema("user");

    // Register operators
    final var addSource = RegistryKey.avro("addSource");
    final var addStatus = RegistryKey.avro("addStatus");

    registry.registerOperator(addSource, AvroMessageProcessor.addFieldOperator("source", SOURCE_APP));
    registry.registerOperator(addStatus, AvroMessageProcessor.addFieldOperator("status", "processed"));

    // Create optimized pipeline
    var pipeline = registry.avroPipelineBuilder("user", 0).add(addSource).add(addStatus).build();

    // Create input record
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "123");

    // Serialize record
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    byte[] input = out.toByteArray();

    // Process message
    byte[] output = pipeline.apply(input);

    // Verify
    AvroMessageProcessor.processAvro(
      output,
      schema,
      r -> {
        assertEquals("123", r.get("id").toString());
        assertEquals(SOURCE_APP, r.get("source").toString());
        assertEquals("processed", r.get("status").toString());
        return r;
      }
    );
  }

  @Test
  void testOptimizedJsonPipelineDefaultOperators() {
    // Create optimized pipeline using default operators
    var pipeline = registry
      .jsonPipelineBuilder()
      .add(MessageProcessorRegistry.JSON_ADD_SOURCE)
      .add(MessageProcessorRegistry.JSON_ADD_TIMESTAMP)
      .add(MessageProcessorRegistry.JSON_MARK_PROCESSED)
      .build();

    // Process message
    byte[] input = "{\"id\":\"123\"}".getBytes(StandardCharsets.UTF_8);
    byte[] output = pipeline.apply(input);

    String result = new String(output, StandardCharsets.UTF_8);
    assertTrue(result.contains("\"source\":\"test-app\""));
    assertTrue(result.contains("\"processed\":\"true\""));
    assertTrue(result.contains("\"timestamp\":"));
    assertTrue(result.contains("\"id\":\"123\""));
  }

  @Test
  void testOptimizedAvroPipelineDefaultOperators() throws java.io.IOException {
    String schemaJson =
      """
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
    Schema schema = AvroMessageProcessor.getSchema("user");

    // Create optimized pipeline using default operators
    var pipeline = registry
      .avroPipelineBuilder("user", 0)
      .add(RegistryKey.avro("addSource_user"))
      .add(RegistryKey.avro("addTimestamp_user"))
      .add(RegistryKey.avro("markProcessed_user"))
      .build();

    // Create input record
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "123");

    // Serialize record
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    byte[] input = out.toByteArray();

    // Process message
    byte[] output = pipeline.apply(input);

    // Verify
    AvroMessageProcessor.processAvro(
      output,
      schema,
      r -> {
        assertEquals("123", r.get("id").toString());
        assertEquals(SOURCE_APP, r.get("source").toString());
        assertEquals("true", r.get("processed").toString());
        assertNotNull(r.get("timestamp"));
        return r;
      }
    );
  }

  @Test
  void testOptimizedAvroPipelineWithOffset() throws java.io.IOException {
    String schemaJson =
      """
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
    Schema schema = AvroMessageProcessor.getSchema("userOffset");

    // Register operators
    final var addSource = RegistryKey.avro("addSource");
    registry.registerOperator(addSource, AvroMessageProcessor.addFieldOperator("source", SOURCE_APP));

    // Create optimized pipeline with offset 5 (simulating magic bytes)
    var pipeline = registry.avroPipelineBuilder("userOffset", 5).add(addSource).build();

    // Create input record
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "123");

    // Serialize record
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    // Add 5 magic bytes
    out.write(new byte[] { 0, 0, 0, 0, 1 });
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    writer.write(record, encoder);
    encoder.flush();
    byte[] input = out.toByteArray();

    // Process message
    byte[] output = pipeline.apply(input);

    // Verify
    AvroMessageProcessor.processAvro(
      output,
      schema,
      r -> {
        assertEquals("123", r.get("id").toString());
        assertEquals(SOURCE_APP, r.get("source").toString());
        return r;
      }
    );
  }
}
