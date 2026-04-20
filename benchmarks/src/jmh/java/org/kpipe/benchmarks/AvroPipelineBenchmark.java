package org.kpipe.benchmarks;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.kpipe.processor.AvroMessageProcessor;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;
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
  private byte[] avroBytes;
  private byte[] avroWithMagicBytes;
  private Function<byte[], byte[]> kpipePipeline;
  private Function<byte[], byte[]> kpipeMagicPipeline;

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
    writer.write(record, EncoderFactory.get().binaryEncoder(out, null));
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

    final var registry = new MessageProcessorRegistry("benchmark-app", MessageFormat.AVRO);
    final var format = MessageFormat.AVRO;
    format.addSchema("user", "com.kpipe.User", schemaJson);
    format.withDefaultSchema("user");

    // Register operators
    final var op1 = RegistryKey.avro("op1");
    final var op2 = RegistryKey.avro("op2");

    registry.register(op1, AvroMessageProcessor.addFieldOperator("processed", true));
    registry.register(op2, AvroMessageProcessor.addFieldOperator("name", "PROCESSED"));

    kpipePipeline = registry.pipeline(format).add(op1, op2).build();
    kpipeMagicPipeline = registry.pipeline(format).skipBytes(5).add(op1, op2).build();
  }

  @Benchmark
  public void kpipeAvroPipeline(final Blackhole bh) {
    bh.consume(kpipePipeline.apply(avroBytes));
  }

  @Benchmark
  public void kpipeAvroMagicPipeline(final Blackhole bh) {
    // This tests the zero-copy offset handling
    bh.consume(kpipeMagicPipeline.apply(avroWithMagicBytes));
  }

  @Benchmark
  public void manualAvroMagicHandling(final Blackhole bh) {
    // This mimics the manual way of handling magic bytes with copying
    final var stripped = Arrays.copyOfRange(avroWithMagicBytes, 5, avroWithMagicBytes.length);
    final var format = MessageFormat.AVRO;
    final var record = format.deserialize(stripped);
    if (record != null) {
      record.put("processed", true);
      record.put("name", "PROCESSED");
    }
    bh.consume(format.serialize(record));
  }
}
