package io.github.eschizoid.kpipe.record;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

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
/// The key and headers change only through [#rekey] and [#withHeader]. Each returns an operator
/// that, when it runs, makes [#current()] return a context with the new key or header for the rest
/// of the binding. The consumed record never changes, and the consumer binds it again for each
/// processing attempt, so a retry starts from the record as it was consumed.
///
/// A batch sink covers many records, so none of them is current there. It reads them with
/// [#currentBatch()], in the same order as its values and with the changes made before each was
/// buffered.
///
/// A context captured with `var ctx = current()` keeps the key and headers it had when captured
/// and works on any thread, including through [#callWith]: each binding keeps its own changes. The
/// consumed key and header arrays a context returns are the record's own: do not modify them,
/// because a retry, the dead-letter topic and `toKafka` read the same ones.
public abstract class RecordContext {

  private static final ScopedValue<Binding> CURRENT = ScopedValue.newInstance();

  /// Bound where no record is current: a batch flush's outcome dispatch and the error handler.
  private static final Binding NONE = new Binding(null, null);

  /// One [#callWith]: the context it bound and what [#rekey] and [#withHeader] made of it since.
  /// Only threads that inherit the binding read or write it. A plain thread or an executor task
  /// does not inherit it; a `StructuredTaskScope` fork does, so a fork binds its own before it
  /// changes the record.
  private static final class Binding {

    private RecordContext current;
    /// The records of the batch being flushed, or null outside a batch sink.
    private final List<RecordContext> batch;

    private Binding(final RecordContext bound, final List<RecordContext> batch) {
      this.current = bound;
      this.batch = batch;
    }
  }

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

  /// @return the record's key, after any [#rekey]; null when it has no key
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
  /// executor task or a plain thread the operator started, inside a batch sink, which reads its
  /// records with [#currentBatch()], and inside the error handler, which receives its record in
  /// the `ProcessingError`
  public static RecordContext current() {
    final var binding = CURRENT.orElse(NONE);
    if (binding.current != null) return binding.current;
    if (binding.batch != null) throw new IllegalStateException(
      "A batch sink covers many records, so none of them is current. Read them with RecordContext.currentBatch(), " +
        "in the same order as the values."
    );
    throw new IllegalStateException(
      "No record is being processed on this thread. Call RecordContext.current() from an operator or sink " +
        "while it runs, or capture it first (var ctx = RecordContext.current()) to use it on another thread."
    );
  }

  /// The records of the batch a batch sink is flushing, in the same order as the values it
  /// receives. Each context is the record as it stood when it was buffered, including any
  /// [#rekey] or [#withHeader].
  ///
  /// @return the batch's contexts, index-aligned with the batch's values; unmodifiable
  /// @throws IllegalStateException outside a batch sink
  public static List<RecordContext> currentBatch() {
    final var batch = CURRENT.orElse(NONE).batch;
    if (batch == null) throw new IllegalStateException(
      "No batch is being flushed on this thread. Call RecordContext.currentBatch() from a batch sink while it runs."
    );
    return batch;
  }

  /// An operator that replaces the record's key for the rest of the current binding, which in the
  /// consumer is one processing attempt. The value passes through unchanged; [#key()] returns the
  /// new key and [#originalKey()] the consumed one. Inside a `StructuredTaskScope` fork, bind the
  /// fork's own context first with `callWith(current(), ...)`, so its changes stay in the fork.
  ///
  /// @param keyFn computes the new key from the value; null means no key
  /// @param <T>   the value type
  /// @return the operator
  /// @throws NullPointerException if `keyFn` is null
  public static <T> UnaryOperator<T> rekey(final Function<? super T, byte[]> keyFn) {
    Objects.requireNonNull(keyFn, "keyFn cannot be null");
    return value -> {
      final var binding = bindingFor("rekey");
      binding.current = new Rekeyed(binding.current, keyFn.apply(value));
      return value;
    };
  }

  /// An operator that adds a header for the rest of the current binding, which in the consumer is
  /// one processing attempt. The value passes through unchanged; [#header] returns the added
  /// value, and [#forEachHeader] reports the header after the consumed record's own. Inside a
  /// `StructuredTaskScope` fork, bind the fork's own context first, as for [#rekey].
  ///
  /// @param name    the header name
  /// @param valueFn computes the header value from the value
  /// @param <T>     the value type
  /// @return the operator
  /// @throws NullPointerException if `name` or `valueFn` is null
  public static <T> UnaryOperator<T> withHeader(final String name, final Function<? super T, byte[]> valueFn) {
    Objects.requireNonNull(name, "name cannot be null");
    Objects.requireNonNull(valueFn, "valueFn cannot be null");
    return value -> {
      final var binding = bindingFor("withHeader");
      binding.current = new WithHeader(binding.current, name, valueFn.apply(value));
      return value;
    };
  }

  /// [#rekey] with the key given as a string and encoded as UTF-8.
  ///
  /// @param keyFn computes the new key from the value; null means no key
  /// @param <T>   the value type
  /// @return the operator
  /// @throws NullPointerException if `keyFn` is null
  public static <T> UnaryOperator<T> rekeyString(final Function<? super T, String> keyFn) {
    Objects.requireNonNull(keyFn, "keyFn cannot be null");
    return rekey(value -> {
      final var key = keyFn.apply(value);
      return key == null ? null : key.getBytes(StandardCharsets.UTF_8);
    });
  }

  private static Binding bindingFor(final String operation) {
    final var binding = CURRENT.orElse(NONE);
    if (binding.current == null) throw new IllegalStateException(
      operation + " changes the record being processed, so it runs only inside a pipeline"
    );
    return binding;
  }

  /// @return whether a record is being processed on this thread
  public static boolean isBound() {
    return CURRENT.orElse(NONE).current != null;
  }

  /// Runs `op` with `context` as the current record. What [#rekey] and [#withHeader] do inside
  /// `op` lasts until `op` returns and changes neither `context` nor any other binding of it. The
  /// consumer calls this once per processing attempt; a test calls it to run an operator that
  /// reads the record.
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
    return ScopedValue.where(CURRENT, new Binding(context, null)).call(op);
  }

  /// Runs `op` as a batch sink: [#currentBatch()] returns `contexts` and [#current()] throws. The
  /// consumer calls this for each flush; a test calls it to run a batch sink that reads its
  /// records.
  ///
  /// @param contexts the batch's records, in the same order as the values `op` flushes
  /// @param op       the work to run
  /// @param <R>      the result type
  /// @param <X>      the exception `op` may throw
  /// @return what `op` returns
  /// @throws X when `op` throws
  /// @throws NullPointerException if `contexts` or one of its elements is null
  public static <R, X extends Throwable> R callWithBatch(
    final List<RecordContext> contexts,
    final ScopedValue.CallableOp<R, X> op
  ) throws X {
    return ScopedValue.where(CURRENT, new Binding(null, List.copyOf(contexts))).call(op);
  }

  /// Runs `op` with no current record, so [#current()] throws inside it. The consumer runs a
  /// batch flush's outcome dispatch and the error handler this way.
  ///
  /// @param op  the work to run
  /// @param <R> the result type
  /// @param <X> the exception `op` may throw
  /// @return what `op` returns
  /// @throws X when `op` throws
  public static <R, X extends Throwable> R callWithout(final ScopedValue.CallableOp<R, X> op) throws X {
    return ScopedValue.where(CURRENT, NONE).call(op);
  }

  /// A context that answers from `base` except where a subclass overrides.
  private abstract static class Derived extends RecordContext {

    final RecordContext base;

    Derived(final RecordContext base) {
      this.base = base;
    }

    @Override
    public String topic() {
      return base.topic();
    }

    @Override
    public int partition() {
      return base.partition();
    }

    @Override
    public long offset() {
      return base.offset();
    }

    @Override
    public long timestamp() {
      return base.timestamp();
    }

    @Override
    public byte[] key() {
      return base.key();
    }

    @Override
    public byte[] originalKey() {
      return base.originalKey();
    }

    @Override
    public Optional<byte[]> header(final String name) {
      return base.header(name);
    }

    @Override
    public void forEachHeader(final BiConsumer<String, byte[]> action) {
      base.forEachHeader(action);
    }
  }

  private static final class Rekeyed extends Derived {

    private final byte[] key;

    Rekeyed(final RecordContext base, final byte[] key) {
      super(base);
      this.key = key;
    }

    @Override
    public byte[] key() {
      return key;
    }
  }

  private static final class WithHeader extends Derived {

    private final String name;
    private final byte[] value;

    WithHeader(final RecordContext base, final String name, final byte[] value) {
      super(base);
      this.name = name;
      this.value = value;
    }

    @Override
    public Optional<byte[]> header(final String header) {
      return name.equals(header) ? Optional.ofNullable(value) : base.header(header);
    }

    @Override
    public void forEachHeader(final BiConsumer<String, byte[]> action) {
      base.forEachHeader(action);
      action.accept(name, value);
    }
  }
}
