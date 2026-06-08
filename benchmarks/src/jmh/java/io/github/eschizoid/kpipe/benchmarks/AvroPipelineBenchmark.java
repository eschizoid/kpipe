package io.github.eschizoid.kpipe.benchmarks;

import io.github.eschizoid.kpipe.format.avro.AvroFormat;
import io.github.eschizoid.kpipe.format.avro.AvroRegistryKey;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.Result;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/// JMH Benchmark for evaluating the efficiency of KPipe's Avro zero-copy magic byte handling.
///
/// This benchmark compares the throughput of our zero-copy offset deserialization
/// strategy against the manual approach of stripping prefixes by copying the entire
/// byte array.
///
/// ### Scenarios:
/// 1. **KPipe Avro Magic Pipeline**: Deserializes from an offset within the original array.
/// 2. **Manual Avro Magic Handling**: Strips the magic prefix using `Arrays.copyOfRange`.
///
/// ### Running the Benchmark:
/// ```bash
/// ./gradlew :benchmarks:jmh -Pjmh.includes='AvroPipelineBenchmark'
/// ```
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class AvroPipelineBenchmark {

  private Schema schema;
  private AvroFormat format;
  private byte[] avroBytes;
  private byte[] avroWithMagicBytes;
  private MessagePipeline<GenericRecord> kpipePipeline;
  private MessagePipeline<GenericRecord> kpipeMagicPipeline;

  @Setup
  public void setup() throws IOException {
    String schemaJson = """
      {
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "name", "type": "string"},
          {"name": "email", "type": ["null", "string"], "default": null},
          {"name": "processed", "type": "boolean", "default": false}
        ]
      }
      """;
    schema = new Schema.Parser().parse(schemaJson);

    final var record = new GenericData.Record(schema);
    record.put("id", 123L);
    record.put("name", "Jane Doe");
    record.put("email", "jane.doe@example.com");
    record.put("processed", false);

    final var out = new ByteArrayOutputStream();
    final var writer = new GenericDatumWriter<>(schema);
    final var encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(record, encoder);
    // BinaryEncoder buffers — without flush(), out.toByteArray() returns empty bytes and the
    // bench produces a non-deserializable payload. The previous snapshot ran fine because
    // an earlier AvroFormat.deserialize codepath happened to ignore the empty input; the v1
    // batch-sink work tightened that contract and surfaced the latent bug.
    encoder.flush();
    avroBytes = out.toByteArray();

    // With magic bytes (Confluent format: 0x0 + 4 bytes schema ID)
    avroWithMagicBytes = new byte[avroBytes.length + 5];
    avroWithMagicBytes[0] = 0;
    // Schema ID = 1 (0,0,0,1)
    avroWithMagicBytes[1] = 0;
    avroWithMagicBytes[2] = 0;
    avroWithMagicBytes[3] = 0;
    avroWithMagicBytes[4] = 1;
    System.arraycopy(avroBytes, 0, avroWithMagicBytes, 5, avroBytes.length);

    format = AvroFormat.of(schemaJson);
    final var registry = new MessageProcessorRegistry();

    // Register operators inline using the native Avro API (operator helpers were removed in
    // 1.11.x).
    final var op1 = AvroRegistryKey.of("op1");
    final var op2 = AvroRegistryKey.of("op2");

    final UnaryOperator<GenericRecord> setProcessed = r -> {
      r.put("processed", true);
      return r;
    };
    final UnaryOperator<GenericRecord> renameProcessed = r -> {
      r.put("name", "PROCESSED");
      return r;
    };

    registry.registerOperator(op1, setProcessed);
    registry.registerOperator(op2, renameProcessed);

    kpipePipeline = registry.pipeline(format).add(op1, op2).build();
    kpipeMagicPipeline = registry.pipeline(format).skipBytes(5).add(op1, op2).build();
  }

  @Benchmark
  public void kpipeAvroPipeline(final Blackhole bh) {
    final var value = kpipePipeline.deserializeOrFail(avroBytes);
    final var result = kpipePipeline.process(value);
    if (!(result instanceof Result.Passed<GenericRecord> passed)) {
      throw new AssertionError("benchmark input should always pass: " + result);
    }
    bh.consume(kpipePipeline.serialize(passed.value()));
  }

  @Benchmark
  public void kpipeAvroMagicPipeline(final Blackhole bh) {
    // This tests the zero-copy offset handling: deserialize starts at byte 5, no copy
    final var value = kpipeMagicPipeline.deserializeOrFail(avroWithMagicBytes);
    final var result = kpipeMagicPipeline.process(value);
    if (!(result instanceof Result.Passed<GenericRecord> passed)) {
      throw new AssertionError("benchmark input should always pass: " + result);
    }
    bh.consume(kpipeMagicPipeline.serialize(passed.value()));
  }

  @Benchmark
  public void manualAvroMagicHandling(final Blackhole bh) {
    // This mimics the manual way of handling magic bytes with copying
    final var stripped = Arrays.copyOfRange(avroWithMagicBytes, 5, avroWithMagicBytes.length);
    final var record = format.deserialize(stripped);
    if (record != null) {
      record.put("processed", true);
      record.put("name", "PROCESSED");
    }
    bh.consume(format.serialize(record));
  }
}
