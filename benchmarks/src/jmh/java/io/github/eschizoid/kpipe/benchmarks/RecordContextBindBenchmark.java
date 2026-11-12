package io.github.eschizoid.kpipe.benchmarks;

import io.github.eschizoid.kpipe.format.json.JsonFormat;
import io.github.eschizoid.kpipe.record.RecordContext;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.RegistryKey;
import io.github.eschizoid.kpipe.registry.Result;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

/// What binding the record in a `ScopedValue` costs per record, next to the JSON pipeline work the
/// record already pays for: deserialize, three operators, serialize.
///
/// - `valueOnly`: the pipeline as it runs without a bound record.
/// - `bound`: the same pipeline inside `RecordContext.callWith`, as every record runs once the
///   consumer binds it, whether or not an operator reads it.
/// - `boundAndRead`: bound, with one operator reading the key and the sink step reading a header.
/// - `boundAndChanged`: bound, with a `rekey` and a `withHeader` before the sink step reads both.
///
/// ```bash
/// ./gradlew :benchmarks:jmh -Pjmh.includes='RecordContextBindBenchmark' -Pjmh.profilers=gc
/// ```
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class RecordContextBindBenchmark {

  private static final long BENCHMARK_TIMESTAMP = 1_700_000_000_000L;
  private static final byte[] PROCESSED_BY = "kpipe".getBytes(StandardCharsets.UTF_8);

  private ConsumerRecord<byte[], byte[]> record;
  private MessagePipeline<Map<String, Object>> valuePipeline;
  private MessagePipeline<Map<String, Object>> readingPipeline;
  private MessagePipeline<Map<String, Object>> changingPipeline;

  @Setup
  public void setup() {
    final var json = """
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
      """.getBytes(StandardCharsets.UTF_8);
    final var headers = new RecordHeaders();
    headers.add("tenant", "acme".getBytes(StandardCharsets.UTF_8));
    record = new ConsumerRecord<>(
      "orders",
      3,
      42L,
      BENCHMARK_TIMESTAMP,
      TimestampType.CREATE_TIME,
      8,
      json.length,
      "order-12345".getBytes(StandardCharsets.UTF_8),
      json,
      headers,
      Optional.empty()
    );

    final var registry = new MessageProcessorRegistry();
    final var op1 = RegistryKey.json("op1");
    final var op2 = RegistryKey.json("op2");
    final var op3 = RegistryKey.json("op3");
    final var readKey = RegistryKey.json("readKey");
    registry.registerOperator(op1, map -> {
      map.put("processed_by", "kpipe");
      return map;
    });
    registry.registerOperator(op2, map -> {
      map.put("timestamp", BENCHMARK_TIMESTAMP);
      return map;
    });
    registry.registerOperator(op3, map -> {
      map.remove("email");
      return map;
    });
    registry.registerOperator(readKey, map -> {
      map.put("key", RecordContext.current().keyString());
      return map;
    });
    valuePipeline = registry.pipeline(JsonFormat.INSTANCE).pipe(op1, op2, op3).build();
    readingPipeline = registry.pipeline(JsonFormat.INSTANCE).pipe(op1, op2, op3, readKey).build();
    changingPipeline = registry
      .pipeline(JsonFormat.INSTANCE)
      .pipe(op1, op2, op3)
      .rekeyString(map -> String.valueOf(map.get("id")))
      .withHeader("processed-by", _ -> PROCESSED_BY)
      .build();
  }

  @Benchmark
  public void valueOnly(final Blackhole bh) {
    bh.consume(run(valuePipeline, record.value()));
  }

  @Benchmark
  public void bound(final Blackhole bh) {
    bh.consume(RecordContext.callWith(new ConsumedRecord(record), () -> run(valuePipeline, record.value())));
  }

  @Benchmark
  public void boundAndRead(final Blackhole bh) {
    bh.consume(
      RecordContext.callWith(new ConsumedRecord(record), () -> {
        final var bytes = run(readingPipeline, record.value());
        return RecordContext.current().header("tenant").isPresent() ? bytes : null;
      })
    );
  }

  @Benchmark
  public void boundAndChanged(final Blackhole bh) {
    bh.consume(
      RecordContext.callWith(new ConsumedRecord(record), () -> {
        final var bytes = run(changingPipeline, record.value());
        final var ctx = RecordContext.current();
        return ctx.key() != null && ctx.header("processed-by").isPresent() ? bytes : null;
      })
    );
  }

  private static byte[] run(final MessagePipeline<Map<String, Object>> pipeline, final byte[] data) {
    final var result = pipeline.process(pipeline.deserializeOrFail(data));
    if (!(result instanceof Result.Passed<Map<String, Object>> passed)) {
      throw new AssertionError("benchmark input should always pass: " + result);
    }
    return JsonFormat.INSTANCE.serialize(passed.value());
  }

  /// The same shape as the consumer's adapter: one object holding the consumed record.
  private static final class ConsumedRecord extends RecordContext {

    private final ConsumerRecord<byte[], byte[]> record;

    ConsumedRecord(final ConsumerRecord<byte[], byte[]> record) {
      this.record = record;
    }

    @Override
    public String topic() {
      return record.topic();
    }

    @Override
    public int partition() {
      return record.partition();
    }

    @Override
    public long offset() {
      return record.offset();
    }

    @Override
    public long timestamp() {
      return record.timestamp();
    }

    @Override
    public byte[] key() {
      return record.key();
    }

    @Override
    public byte[] originalKey() {
      return record.key();
    }

    @Override
    public Optional<byte[]> header(final String name) {
      final var header = record.headers().lastHeader(name);
      return header == null ? Optional.empty() : Optional.ofNullable(header.value());
    }

    @Override
    public void forEachHeader(final BiConsumer<String, byte[]> action) {
      for (final var header : record.headers()) action.accept(header.key(), header.value());
    }
  }
}
