package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.tracing.Tracer;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/// Every consumer-wide setting, each declared once: its default, its validation and its error
/// message live here, and [KPipeConsumerBuilder], `Stream` and `MultiBuilder` all delegate to it,
/// so the three surfaces cannot drift.
///
/// Immutable, with final fields: each `with*` method checks its arguments and returns a new
/// instance with that setting changed and recorded in [#explicitlySet()]. A setting counts as set
/// once its method is called, even with the default value, which is how a `MultiBuilder` route
/// that calls a consumer-wide method on its own `Stream` is told apart from one that does not.
///
/// ```java
/// final var settings = ConsumerSettings.defaults()
///   .withRetry(3, Duration.ofMillis(200))
///   .withDeadLetterTopic("orders-dlq");
/// ```
public final class ConsumerSettings {

  /// The consumer-wide settings, in the order a route's misplaced settings are reported. Each
  /// names the method that sets it.
  public enum Setting {
    /// [#withRetry(int, Duration)].
    RETRY("withRetry"),
    /// [#withBackpressure()] and [#withBackpressure(long, long)].
    BACKPRESSURE("withBackpressure"),
    /// [#withProcessingMode(ProcessingMode)].
    PROCESSING_MODE("withProcessingMode"),
    /// [#withKeyOrderedMaxKeys(int)].
    KEY_ORDERED_MAX_KEYS("withKeyOrderedMaxKeys"),
    /// [#withMetrics(MetricsExporter)].
    METRICS("withMetrics"),
    /// [#onAttemptFailed(Consumer)].
    ATTEMPT_FAILED("onAttemptFailed"),
    /// [#withErrorHandler(Consumer)].
    ERROR_HANDLER("withErrorHandler"),
    /// [#withDeadLetterTopic(String)].
    DEAD_LETTER_TOPIC("withDeadLetterTopic"),
    /// [#withPollTimeout(Duration)].
    POLL_TIMEOUT("withPollTimeout"),
    /// [#withTracer(Tracer)].
    TRACER("withTracer"),
    /// [#withCircuitBreaker(CircuitBreakerController)].
    CIRCUIT_BREAKER("withCircuitBreaker"),
    /// [#withMetricsLog(Duration)].
    METRICS_LOG("withMetricsLog"),
    /// [#withShutdownHook(boolean)].
    SHUTDOWN_HOOK("withShutdownHook"),
    /// [#withThreadTerminationTimeout(Duration)].
    THREAD_TERMINATION_TIMEOUT("withThreadTerminationTimeout"),
    /// [#withWaitForMessagesTimeout(Duration)].
    WAIT_FOR_MESSAGES_TIMEOUT("withWaitForMessagesTimeout"),
    /// [#withCommitInterval(Duration)].
    COMMIT_INTERVAL("withCommitInterval"),
    /// [#withOffsetStore(OffsetStore)].
    OFFSET_STORE("withOffsetStore");

    private final String method;

    Setting(final String method) {
      this.method = method;
    }

    /// @return the name of the method that sets this setting, such as `withRetry`
    public String method() {
      return method;
    }
  }

  private static final Logger FAILURE_LOG = System.getLogger(KPipeConsumerBuilder.class.getName());

  private static final KPipeConsumer.ErrorHandler LOG_FAILURE = e ->
    FAILURE_LOG.log(
      Level.WARNING,
      "Failed at offset {0} after {1} retries: {2}",
      e.record().offset(),
      e.retryCount(),
      e.exception().getMessage()
    );

  private static final ConsumerSettings DEFAULTS = new ConsumerSettings(new Values());

  private final EnumSet<Setting> explicitlySet;
  private final int maxRetries;
  private final Duration retryBackoff;
  private final long backpressureHigh;
  private final long backpressureLow;
  private final ProcessingMode processingMode;
  private final int keyOrderedMaxKeys;
  private final MetricsExporter metricsExporter;
  private final KPipeConsumer.ErrorHandler attemptFailedObserver;
  private final KPipeConsumer.ErrorHandler errorHandler;
  private final String deadLetterTopic;
  private final Duration pollTimeout;
  private final Tracer tracer;
  private final CircuitBreakerController circuitBreaker;
  private final Duration metricsLogInterval;
  private final boolean shutdownHook;
  private final Duration threadTerminationTimeout;
  private final Duration waitForMessagesTimeout;
  private final Duration commitInterval;
  private final OffsetStore offsetStore;

  private ConsumerSettings(final Values values) {
    this.explicitlySet = EnumSet.copyOf(values.explicitlySet);
    this.maxRetries = values.maxRetries;
    this.retryBackoff = values.retryBackoff;
    this.backpressureHigh = values.backpressureHigh;
    this.backpressureLow = values.backpressureLow;
    this.processingMode = values.processingMode;
    this.keyOrderedMaxKeys = values.keyOrderedMaxKeys;
    this.metricsExporter = values.metricsExporter;
    this.attemptFailedObserver = values.attemptFailedObserver;
    this.errorHandler = values.errorHandler;
    this.deadLetterTopic = values.deadLetterTopic;
    this.pollTimeout = values.pollTimeout;
    this.tracer = values.tracer;
    this.circuitBreaker = values.circuitBreaker;
    this.metricsLogInterval = values.metricsLogInterval;
    this.shutdownHook = values.shutdownHook;
    this.threadTerminationTimeout = values.threadTerminationTimeout;
    this.waitForMessagesTimeout = values.waitForMessagesTimeout;
    this.commitInterval = values.commitInterval;
    this.offsetStore = values.offsetStore;
  }

  /// @return the settings a consumer gets when none is set
  public static ConsumerSettings defaults() {
    return DEFAULTS;
  }

  /// Retries a failed record up to `maxRetries` times, waiting `backoff` before each retry. A
  /// `NonRetryableException`, or a failure caused by one, is not retried. The default is no
  /// retry.
  ///
  /// @param maxRetries how many times to retry, zero or more
  /// @param backoff    the wait before each retry, zero or more
  /// @return settings with the retry policy changed
  /// @throws IllegalArgumentException if `maxRetries` or `backoff` is negative
  /// @throws NullPointerException if `backoff` is null
  public ConsumerSettings withRetry(final int maxRetries, final Duration backoff) {
    if (maxRetries < 0) throw new IllegalArgumentException("maxRetries cannot be negative, got " + maxRetries);
    Objects.requireNonNull(backoff, "backoff cannot be null");
    if (backoff.isNegative()) throw new IllegalArgumentException("backoff cannot be negative, got " + backoff);
    return with(Setting.RETRY, s -> {
      s.maxRetries = maxRetries;
      s.retryBackoff = backoff;
    });
  }

  /// Keeps backpressure on with its default watermarks. Backpressure is on by default; this
  /// records the choice explicitly.
  ///
  /// @return settings with backpressure set to its default watermarks
  public ConsumerSettings withBackpressure() {
    return withBackpressure(
      BackpressureController.DEFAULT_HIGH_WATERMARK,
      BackpressureController.DEFAULT_LOW_WATERMARK
    );
  }

  /// Pauses fetching when the backpressure metric reaches `high` and resumes once it falls to
  /// `low`. The metric is records in flight, or the consumer's lag in `SEQUENTIAL` mode.
  ///
  /// @param high the watermark that pauses, greater than zero
  /// @param low  the watermark that resumes, zero or more and less than `high`
  /// @return settings with the watermarks changed
  /// @throws IllegalArgumentException if the watermarks are out of range
  public ConsumerSettings withBackpressure(final long high, final long low) {
    if (high <= 0 || low < 0 || low >= high) throw new IllegalArgumentException(
      "withBackpressure(%d, %d): the high watermark must be positive and the low one at least 0 and below it".formatted(
        high,
        low
      )
    );
    return with(Setting.BACKPRESSURE, s -> {
      s.backpressureHigh = high;
      s.backpressureLow = low;
    });
  }

  /// Selects how records are dispatched; see [ProcessingMode]. The default is `PARALLEL`.
  ///
  /// @param mode the processing mode
  /// @return settings with the mode changed
  /// @throws NullPointerException if `mode` is null
  public ConsumerSettings withProcessingMode(final ProcessingMode mode) {
    Objects.requireNonNull(mode, "mode cannot be null");
    return with(Setting.PROCESSING_MODE, s -> s.processingMode = mode);
  }

  /// Caps the distinct keys held at once in `KEY_ORDERED` mode. Other modes ignore it, and the
  /// consumer logs a warning when it is set for one.
  ///
  /// @param maxKeys the cap, greater than zero
  /// @return settings with the cap changed
  /// @throws IllegalArgumentException if `maxKeys` is not positive
  public ConsumerSettings withKeyOrderedMaxKeys(final int maxKeys) {
    if (maxKeys <= 0) throw new IllegalArgumentException("maxKeys must be positive, got " + maxKeys);
    return with(Setting.KEY_ORDERED_MAX_KEYS, s -> s.keyOrderedMaxKeys = maxKeys);
  }

  /// Exports the consumer's metrics to a monitoring backend. Without one the metrics are still
  /// counted and readable through [KPipeConsumer#metrics()].
  ///
  /// @param exporter the exporter
  /// @return settings with the exporter changed
  /// @throws NullPointerException if `exporter` is null
  public ConsumerSettings withMetrics(final MetricsExporter exporter) {
    Objects.requireNonNull(exporter, "metrics exporter cannot be null");
    return with(Setting.METRICS, s -> s.metricsExporter = exporter);
  }

  /// Calls `observer` for every failed processing attempt, retries included. It only watches: it
  /// cannot stop a retry or the error handling that follows, and one that throws is logged.
  ///
  /// @param observer called with each failed attempt
  /// @return settings with the observer changed
  /// @throws NullPointerException if `observer` is null
  public ConsumerSettings onAttemptFailed(final Consumer<? super KPipeConsumer.ProcessingError> observer) {
    Objects.requireNonNull(observer, "observer cannot be null");
    return with(Setting.ATTEMPT_FAILED, s -> s.attemptFailedObserver = handler(observer));
  }

  /// Calls `handler` once for each record that fails for good. The default logs a warning.
  ///
  /// @param handler called with each failed record
  /// @return settings with the handler changed
  /// @throws NullPointerException if `handler` is null
  public ConsumerSettings withErrorHandler(final Consumer<? super KPipeConsumer.ProcessingError> handler) {
    Objects.requireNonNull(handler, "handler cannot be null");
    return with(Setting.ERROR_HANDLER, s -> s.errorHandler = handler(handler));
  }

  /// Sends records that fail for good to `topic`.
  ///
  /// @param topic the dead-letter topic
  /// @return settings with the topic changed
  /// @throws NullPointerException if `topic` is null
  /// @throws IllegalArgumentException if `topic` is blank
  public ConsumerSettings withDeadLetterTopic(final String topic) {
    Objects.requireNonNull(topic, "topic cannot be null");
    if (topic.isBlank()) throw new IllegalArgumentException("topic cannot be blank");
    return with(Setting.DEAD_LETTER_TOPIC, s -> s.deadLetterTopic = topic);
  }

  /// Sets how long each poll waits for records. The default is 100 milliseconds.
  ///
  /// @param timeout the poll timeout, greater than zero
  /// @return settings with the timeout changed
  /// @throws NullPointerException if `timeout` is null
  /// @throws IllegalArgumentException if `timeout` is not positive
  public ConsumerSettings withPollTimeout(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    if (!timeout.isPositive()) throw new IllegalArgumentException("timeout must be positive, got " + timeout);
    return with(Setting.POLL_TIMEOUT, s -> s.pollTimeout = timeout);
  }

  /// Propagates trace context across the Kafka boundary. The default is [Tracer#noop()].
  ///
  /// @param tracer the tracer
  /// @return settings with the tracer changed
  /// @throws NullPointerException if `tracer` is null
  public ConsumerSettings withTracer(final Tracer tracer) {
    Objects.requireNonNull(tracer, "tracer cannot be null");
    return with(Setting.TRACER, s -> s.tracer = tracer);
  }

  /// Pauses consumption while the failure rate is over the controller's threshold.
  ///
  /// @param controller the breaker policy
  /// @return settings with the breaker changed
  /// @throws NullPointerException if `controller` is null
  public ConsumerSettings withCircuitBreaker(final CircuitBreakerController controller) {
    Objects.requireNonNull(controller, "controller cannot be null");
    return with(Setting.CIRCUIT_BREAKER, s -> s.circuitBreaker = controller);
  }

  /// Logs one line of metrics every `interval`, plus a final line at close.
  ///
  /// @param interval how often to log, greater than zero
  /// @return settings with the interval changed
  /// @throws NullPointerException if `interval` is null
  /// @throws IllegalArgumentException if `interval` is not positive
  public ConsumerSettings withMetricsLog(final Duration interval) {
    Objects.requireNonNull(interval, "interval cannot be null");
    if (!interval.isPositive()) throw new IllegalArgumentException("interval must be positive, got " + interval);
    return with(Setting.METRICS_LOG, s -> s.metricsLogInterval = interval);
  }

  /// Registers a JVM shutdown hook that closes the consumer. The default is no hook.
  ///
  /// @param shutdownHook whether to register the hook
  /// @return settings with the choice changed
  public ConsumerSettings withShutdownHook(final boolean shutdownHook) {
    return with(Setting.SHUTDOWN_HOOK, s -> s.shutdownHook = shutdownHook);
  }

  /// Sets how long close waits for the consumer thread to finish. Unset, it depends on the
  /// processing mode; see [ConsumerDefaults#threadTermination].
  ///
  /// @param timeout the wait, zero or more
  /// @return settings with the timeout changed
  /// @throws NullPointerException if `timeout` is null
  /// @throws IllegalArgumentException if `timeout` is negative
  public ConsumerSettings withThreadTerminationTimeout(final Duration timeout) {
    return with(Setting.THREAD_TERMINATION_TIMEOUT, s -> s.threadTerminationTimeout = notNegative(timeout));
  }

  /// Sets how long close waits for dispatched records to finish. The default is 5 seconds.
  ///
  /// @param timeout the wait, zero or more
  /// @return settings with the timeout changed
  /// @throws NullPointerException if `timeout` is null
  /// @throws IllegalArgumentException if `timeout` is negative
  public ConsumerSettings withWaitForMessagesTimeout(final Duration timeout) {
    return with(Setting.WAIT_FOR_MESSAGES_TIMEOUT, s -> s.waitForMessagesTimeout = notNegative(timeout));
  }

  /// Sets how often the default offset manager commits. The default is 30 seconds. A custom
  /// [OffsetManager] sets its own cadence, and building a consumer with both is rejected.
  ///
  /// @param interval the commit interval, between 1 millisecond and `Long.MAX_VALUE` milliseconds
  /// @return settings with the interval changed
  /// @throws NullPointerException if `interval` is null
  /// @throws IllegalArgumentException if `interval` is out of range
  public ConsumerSettings withCommitInterval(final Duration interval) {
    Objects.requireNonNull(interval, "interval cannot be null");
    // The offset manager schedules in whole milliseconds.
    if (
      interval.compareTo(Duration.ofMillis(1)) < 0 || interval.compareTo(Duration.ofMillis(Long.MAX_VALUE)) > 0
    ) throw new IllegalArgumentException("interval must be between 1ms and Long.MAX_VALUE ms, got " + interval);
    return with(Setting.COMMIT_INTERVAL, s -> s.commitInterval = interval);
  }

  /// Keeps committed offsets in `store` instead of Kafka. A custom [OffsetManager] stores its own
  /// offsets, and building a consumer with both is rejected.
  ///
  /// @param store the offset store
  /// @return settings with the store changed
  /// @throws NullPointerException if `store` is null
  public ConsumerSettings withOffsetStore(final OffsetStore store) {
    Objects.requireNonNull(store, "store cannot be null");
    return with(Setting.OFFSET_STORE, s -> s.offsetStore = store);
  }

  /// @return the settings whose method was called, in declaration order; unmodifiable
  public Set<Setting> explicitlySet() {
    return Collections.unmodifiableSet(explicitlySet);
  }

  /// @return how many times a failed record is retried
  public int maxRetries() {
    return maxRetries;
  }

  /// @return the wait before each retry
  public Duration retryBackoff() {
    return retryBackoff;
  }

  /// @return the backpressure watermark that pauses
  public long backpressureHigh() {
    return backpressureHigh;
  }

  /// @return the backpressure watermark that resumes
  public long backpressureLow() {
    return backpressureLow;
  }

  /// @return the processing mode
  public ProcessingMode processingMode() {
    return processingMode;
  }

  /// @return the distinct-key cap for `KEY_ORDERED` mode
  public int keyOrderedMaxKeys() {
    return keyOrderedMaxKeys;
  }

  /// @return the metrics exporter, or null when none is set
  public MetricsExporter metricsExporter() {
    return metricsExporter;
  }

  /// @return the attempt observer, or null when none is set
  public KPipeConsumer.ErrorHandler attemptFailedObserver() {
    return attemptFailedObserver;
  }

  /// @return the error handler; the default logs a warning
  public KPipeConsumer.ErrorHandler errorHandler() {
    return errorHandler;
  }

  /// @return the dead-letter topic, or null when none is set
  public String deadLetterTopic() {
    return deadLetterTopic;
  }

  /// @return the poll timeout
  public Duration pollTimeout() {
    return pollTimeout;
  }

  /// @return the tracer; [Tracer#noop()] when none is set
  public Tracer tracer() {
    return tracer;
  }

  /// @return the circuit breaker, or null when none is set
  public CircuitBreakerController circuitBreaker() {
    return circuitBreaker;
  }

  /// @return the metrics log interval, or null when metrics are not logged
  public Duration metricsLogInterval() {
    return metricsLogInterval;
  }

  /// @return whether a JVM shutdown hook closes the consumer
  public boolean shutdownHook() {
    return shutdownHook;
  }

  /// @return the thread-termination timeout, or null for the processing mode's default
  public Duration threadTerminationTimeout() {
    return threadTerminationTimeout;
  }

  /// @return how long close waits for dispatched records
  public Duration waitForMessagesTimeout() {
    return waitForMessagesTimeout;
  }

  /// @return the commit interval, or null for the default offset manager's own
  public Duration commitInterval() {
    return commitInterval;
  }

  /// @return the offset store, or null when offsets are committed to Kafka
  public OffsetStore offsetStore() {
    return offsetStore;
  }

  /// Returns new settings with `change` applied and `setting` recorded as set.
  private ConsumerSettings with(final Setting setting, final Consumer<Values> change) {
    final var values = new Values(this);
    change.accept(values);
    values.explicitlySet.add(setting);
    return new ConsumerSettings(values);
  }

  private static KPipeConsumer.ErrorHandler handler(final Consumer<? super KPipeConsumer.ProcessingError> consumer) {
    return consumer instanceof KPipeConsumer.ErrorHandler handler ? handler : consumer::accept;
  }

  private static Duration notNegative(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    if (timeout.isNegative()) throw new IllegalArgumentException("timeout cannot be negative, got " + timeout);
    return timeout;
  }

  /// The mutable copy a `with*` method changes before it becomes a new [ConsumerSettings].
  private static final class Values {

    EnumSet<Setting> explicitlySet = EnumSet.noneOf(Setting.class);
    int maxRetries = 0;
    Duration retryBackoff = Duration.ofMillis(500);
    long backpressureHigh = BackpressureController.DEFAULT_HIGH_WATERMARK;
    long backpressureLow = BackpressureController.DEFAULT_LOW_WATERMARK;
    ProcessingMode processingMode = ProcessingMode.PARALLEL;
    int keyOrderedMaxKeys = ProcessingMode.DEFAULT_KEY_ORDERED_MAX_KEYS;
    MetricsExporter metricsExporter;
    KPipeConsumer.ErrorHandler attemptFailedObserver;
    KPipeConsumer.ErrorHandler errorHandler = LOG_FAILURE;
    String deadLetterTopic;
    Duration pollTimeout = ConsumerDefaults.POLL_TIMEOUT;
    Tracer tracer = Tracer.noop();
    CircuitBreakerController circuitBreaker;
    Duration metricsLogInterval;
    boolean shutdownHook = false;
    Duration threadTerminationTimeout;
    Duration waitForMessagesTimeout = ConsumerDefaults.WAIT_FOR_MESSAGES;
    Duration commitInterval;
    OffsetStore offsetStore;

    Values() {}

    Values(final ConsumerSettings settings) {
      explicitlySet = EnumSet.copyOf(settings.explicitlySet);
      maxRetries = settings.maxRetries;
      retryBackoff = settings.retryBackoff;
      backpressureHigh = settings.backpressureHigh;
      backpressureLow = settings.backpressureLow;
      processingMode = settings.processingMode;
      keyOrderedMaxKeys = settings.keyOrderedMaxKeys;
      metricsExporter = settings.metricsExporter;
      attemptFailedObserver = settings.attemptFailedObserver;
      errorHandler = settings.errorHandler;
      deadLetterTopic = settings.deadLetterTopic;
      pollTimeout = settings.pollTimeout;
      tracer = settings.tracer;
      circuitBreaker = settings.circuitBreaker;
      metricsLogInterval = settings.metricsLogInterval;
      shutdownHook = settings.shutdownHook;
      threadTerminationTimeout = settings.threadTerminationTimeout;
      waitForMessagesTimeout = settings.waitForMessagesTimeout;
      commitInterval = settings.commitInterval;
      offsetStore = settings.offsetStore;
    }
  }
}
