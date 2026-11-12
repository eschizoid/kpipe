package io.github.eschizoid.kpipe.record;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

/// The Kafka record a pipeline is processing: its key, headers, topic, partition, offset and
/// timestamp. While the consumer processes a record, any operator or sink reads it with
/// [#current()]; the value itself still travels as the lambda's argument.
///
/// ```java
/// import static io.github.eschizoid.kpipe.record.RecordContext.current;
///
/// KPipe.json("orders", props)
///     .filter(order -> current().headerString("tenant").isPresent())
///     .toCustom(order -> db.put(current().keyString(), order));
/// ```
///
/// A context reads the consumed record and holds no per-thread state, so one captured with
/// `var ctx = current()` works on any thread. The byte arrays it returns are the record's own: do
/// not modify them, because a retry, the dead-letter topic and `toKafka` read the same ones. The
/// consumer binds the record for each processing attempt, so a retry reads the record as it was
/// consumed.
public abstract class RecordContext {

  private static final ScopedValue<RecordContext> CURRENT = ScopedValue.newInstance();

  /// Bound where no single record is current: a batch flush and the error handler.
  private static final RecordContext NONE = new RecordContext() {
    @Override
    public String topic() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int partition() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long offset() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long timestamp() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] key() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] originalKey() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<byte[]> header(final String name) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void forEachHeader(final BiConsumer<String, byte[]> action) {
      throw new UnsupportedOperationException();
    }
  };

  /// For implementations that adapt a consumed record.
  protected RecordContext() {}

  /// @return the topic the record was consumed from
  public abstract String topic();

  /// @return the partition the record was consumed from
  public abstract int partition();

  /// @return the record's offset
  public abstract long offset();

  /// @return the record's timestamp, in epoch milliseconds
  public abstract long timestamp();

  /// @return the record's key; null when the record has no key
  public abstract byte[] key();

  /// @return the key the record was consumed with; null when it had none
  public abstract byte[] originalKey();

  /// @param name the header name
  /// @return the last value of the header, or empty when the record does not carry it or its last
  /// value is null
  public abstract Optional<byte[]> header(String name);

  /// Calls `action` for each header, in order.
  ///
  /// @param action receives each header's name and value
  public abstract void forEachHeader(BiConsumer<String, byte[]> action);

  /// @return the key decoded as UTF-8; null when the record has no key
  public final String keyString() {
    final var key = key();
    return key == null ? null : new String(key, StandardCharsets.UTF_8);
  }

  /// @param name the header name
  /// @return the last value of the header decoded as UTF-8, or empty when [#header] is empty
  public final Optional<String> headerString(final String name) {
    return header(name).map(value -> new String(value, StandardCharsets.UTF_8));
  }

  /// The record being processed on this thread.
  ///
  /// @return the current record's context
  /// @throws IllegalStateException when no record is being processed on this thread: inside an
  /// executor task or a thread the operator started, inside a batch sink, which covers many
  /// records, and inside the error handler, which receives its record in the `ProcessingError`
  public static RecordContext current() {
    final var context = CURRENT.orElse(NONE);
    if (context == NONE) throw new IllegalStateException(
      "No record is being processed on this thread. Call RecordContext.current() from an operator or sink " +
        "while it runs, or capture it first (var ctx = RecordContext.current()) to use it on another thread."
    );
    return context;
  }

  /// @return whether a record is being processed on this thread
  public static boolean isBound() {
    return CURRENT.orElse(NONE) != NONE;
  }

  /// Runs `op` with `context` as the current record. The consumer calls this once per processing
  /// attempt; a test calls it to run an operator that reads the record.
  ///
  /// @param context the record to bind
  /// @param op      the work to run
  /// @param <R>     the result type
  /// @param <X>     the exception `op` may throw
  /// @return what `op` returns
  /// @throws X when `op` throws
  public static <R, X extends Throwable> R callWith(final RecordContext context, final ScopedValue.CallableOp<R, X> op)
    throws X {
    Objects.requireNonNull(context, "context cannot be null");
    return ScopedValue.where(CURRENT, context).call(op);
  }

  /// Runs `op` with no current record, so [#current()] throws inside it. The consumer runs batch
  /// sinks this way.
  ///
  /// @param op  the work to run
  /// @param <R> the result type
  /// @param <X> the exception `op` may throw
  /// @return what `op` returns
  /// @throws X when `op` throws
  public static <R, X extends Throwable> R callWithout(final ScopedValue.CallableOp<R, X> op) throws X {
    return ScopedValue.where(CURRENT, NONE).call(op);
  }
}
