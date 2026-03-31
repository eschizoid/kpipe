package org.kpipe.benchmarks;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/// JMH Benchmark for evaluating the efficiency of KPipe's JSON processing pipeline.
///
/// This benchmark compares KPipe's "Single SerDe Cycle" optimization against traditional
/// byte-to-byte transformation chaining. It quantifies the "SerDe tax" by measuring the
/// throughput of multiple transformation steps.
///
/// ### Scenarios:
/// 1. **KPipe JSON Pipeline**: Deserializes once, applies multiple operators, and serializes once.
/// 2. **Manual SerDe Chained**: Redundant deserialization/serialization at every step.
/// 3. **Manual Single SerDe**: Typical single-block processing without the pipeline abstraction.
///
/// ### Running the Benchmark:
/// ```bash
/// ./gradlew :benchmarks:jmh -Pjmh.includes='JsonPipelineBenchmark'
/// ```
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JsonPipelineBenchmark {

  private static final long BENCHMARK_TIMESTAMP = 1_700_000_000_000L;

  private byte[] jsonBytes;
  private Function<byte[], byte[]> kpipePipeline;

  @Setup
  public void setup() {
    jsonBytes =
      """
            {
              "id": 12345,
              "name": "John Doe",
              "email": "john.doe@example.com",
              "active": true,
              "balance": 1250.50,
              "tags": ["customer", "premium"],
              "metadata": {
                "source": "mobile",
                "version": "1.2.3"
              }
            }
            """.getBytes(
          StandardCharsets.UTF_8
        );

    final var registry = new MessageProcessorRegistry("benchmark-app", MessageFormat.JSON);
    // Register some operators
    final var op1 = RegistryKey.json("op1");
    final var op2 = RegistryKey.json("op2");
    final var op3 = RegistryKey.json("op3");

    registry.registerOperator(
      op1,
      map -> {
        map.put("processed_by", "kpipe");
        return map;
      }
    );
    registry.registerOperator(
      op2,
      map -> {
        map.put("timestamp", BENCHMARK_TIMESTAMP);
        return map;
      }
    );
    registry.registerOperator(
      op3,
      map -> {
        map.remove("email");
        return map;
      }
    );

    kpipePipeline = registry.pipeline(MessageFormat.JSON).add(op1).add(op2).add(op3).build();
  }

  @Benchmark
  public void kpipeJsonPipeline(final Blackhole bh) {
    bh.consume(kpipePipeline.apply(jsonBytes));
  }

  @Benchmark
  public void manualJsonSerDeChained(final Blackhole bh) {
    // This mimics the "bad" way of chaining byte-to-byte functions
    final var format = MessageFormat.JSON;
    
    // Step 1
    final var map1 = format.deserialize(jsonBytes);
    if (map1 != null) {
        map1.put("processed_by", "manual");
    }
    final var step1 = format.serialize(map1);
    
    // Step 2
    final var map2 = format.deserialize(step1);
    if (map2 != null) {
        map2.put("timestamp", BENCHMARK_TIMESTAMP);
    }
    final var step2 = format.serialize(map2);
    
    // Step 3
    final var map3 = format.deserialize(step2);
    if (map3 != null) {
        map3.remove("email");
    }
    final var step3 = format.serialize(map3);
    
    bh.consume(step3);
  }

  @Benchmark
  public void manualJsonSingleSerDe(final Blackhole bh) {
    // Most people use it like: consumer.poll(record -> { ... logic ... });
    // So the "competitor" is really KPipe's single SerDe vs manual deserialization.

    // Let's benchmark a typical usage pattern:
    // 1. Deserialization (Jackson/DslJson/etc.)
    // 2. Logic
    // 3. Serialization

    final var format = MessageFormat.JSON;
    final var map = format.deserialize(jsonBytes);
    if (map != null) {
        map.put("processed_by", "manual");
        map.put("timestamp", BENCHMARK_TIMESTAMP);
        map.remove("email");
    }
    final var result = format.serialize(map);
    bh.consume(result);
  }
}
