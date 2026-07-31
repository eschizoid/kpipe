package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.CircuitBreakerController;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.KPipeConsumerBuilder;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

/// Package-private immutable holder for every consumer-wide setting the fluent facade exposes:
/// retry, backpressure, processing mode, key-ordered cap, metrics, error handler, dead-letter
/// topic, poll timeout, tracer, and circuit breaker. One `KPipeConsumer` carries one of these,
/// whether it was built from a single [Stream] or folded out of N [MultiBuilder] routes.
///
/// This type is the single registration point for a consumer-wide setting. Before it existed the
/// same list was mirrored across five sites (the [DefaultStream] components + withers, its apply
/// chain, the [MultiBuilder] fields + withers, its re-implemented apply chain, and the per-route
/// rejection checks), and the mirrors drifted — the batch path once silently dropped tracer and
/// circuit breaker. Adding a setting now means: a component here (+ [Mut] line), one line in
/// [#applyTo], one [#CONSUMER_WIDE_SETTINGS] descriptor, and the thin `with*` delegates on
/// [Stream]/[MultiBuilder].
///
/// Nullability encodes "unset": reference components left `null` (and `maxRetries == 0`,
/// `processingMode == PARALLEL`, `keyOrderedMaxKeys == DEFAULT_KEY_ORDERED_MAX_KEYS`) mean the
/// underlying builder keeps its own default.
record ConsumerConfig(
  int maxRetries,
  Duration retryBackoff,
  Long backpressureHigh,
  Long backpressureLow,
  ProcessingMode processingMode,
  int keyOrderedMaxKeys,
  ConsumerMetrics consumerMetrics,
  Consumer<KPipeConsumer.ProcessingError> errorHandler,
  String deadLetterTopic,
  Duration pollTimeout,
  Tracer tracer,
  CircuitBreakerController circuitBreaker
) {
  /// The all-unset configuration: no retry, no backpressure, parallel mode with the default
  /// key-ordered cap, and every optional component `null`.
  static ConsumerConfig defaults() {
    return new ConsumerConfig(
      0,
      Duration.ofMillis(500),
      null,
      null,
      ProcessingMode.PARALLEL,
      ProcessingMode.DEFAULT_KEY_ORDERED_MAX_KEYS,
      null,
      null,
      null,
      null,
      null,
      null
    );
  }

  /// Single funnel for updates: snapshot into a [Mut], let the caller change what they need,
  /// rebuild a new immutable record. Same shape as `DefaultStream.mutate`.
  ConsumerConfig with(final Consumer<Mut> change) {
    final var m = Mut.from(this);
    change.accept(m);
    return m.build();
  }

  /// Applies every set (non-default) setting onto `builder`. This is THE apply chain — both
  /// single-stream sinks ([DefaultSink] / [DefaultBatchSink]) and [MultiBuilder#start()] call it,
  /// so a new setting wired here reaches all three paths at once. Setter order is irrelevant to
  /// the builder (all cross-setting derivation happens in its `build()`), so one fixed order
  /// serves every caller.
  void applyTo(final KPipeConsumerBuilder builder) {
    builder.withProcessingMode(processingMode);
    builder.withKeyOrderedMaxKeys(keyOrderedMaxKeys);
    if (maxRetries > 0) builder.withRetry(maxRetries, retryBackoff);
    if (backpressureHigh != null) builder.withBackpressure(backpressureHigh, backpressureLow);
    if (consumerMetrics != null) builder.withMetrics(consumerMetrics);
    if (errorHandler != null) builder.withErrorHandler(errorHandler::accept);
    if (deadLetterTopic != null) builder.withDeadLetterTopic(deadLetterTopic);
    if (pollTimeout != null) builder.withPollTimeout(pollTimeout);
    if (tracer != null) builder.withTracer(tracer);
    if (circuitBreaker != null) builder.withCircuitBreaker(circuitBreaker);
  }

  /// One consumer-wide setting as seen by the [MultiBuilder] per-route guard: the `Stream.with*`
  /// name, a predicate telling whether a route's config sets it, and the rejection message
  /// pointing the user at the symmetric `MultiBuilder.with*` mirror.
  record ConsumerWideSetting(
    String setting,
    Predicate<ConsumerConfig> isSet,
    BiFunction<String, ConsumerConfig, String> rejection
  ) {}

  /// Descriptor per consumer-wide setting, iterated by
  /// `MultiBuilder.rejectPerRouteConsumerWideSettings` instead of one hand-written check per
  /// setting. A setting registered here can never be silently dropped by a route configurator.
  /// Order is the reporting order when a route sets several at once.
  static final List<ConsumerWideSetting> CONSUMER_WIDE_SETTINGS = List.of(
    new ConsumerWideSetting(
      "withProcessingMode",
      c -> c.processingMode() != ProcessingMode.PARALLEL,
      (topic, c) ->
        "Route '%s' sets withProcessingMode(%s) on its Stream, but processing mode is a consumer-wide setting. ".formatted(
          topic,
          c.processingMode()
        ) +
        "Move the call to MultiBuilder.withProcessingMode(...) instead."
    ),
    new ConsumerWideSetting(
      "withKeyOrderedMaxKeys",
      c -> c.keyOrderedMaxKeys() != ProcessingMode.DEFAULT_KEY_ORDERED_MAX_KEYS,
      (topic, c) ->
        "Route '%s' sets withKeyOrderedMaxKeys(%d) on its Stream, but the key-ordered key cap is a consumer-wide setting. ".formatted(
          topic,
          c.keyOrderedMaxKeys()
        ) +
        "Move the call to MultiBuilder.withKeyOrderedMaxKeys(...) instead."
    ),
    mirrored("withMetrics", c -> c.consumerMetrics() != null),
    mirrored("withTracer", c -> c.tracer() != null),
    mirrored("withCircuitBreaker", c -> c.circuitBreaker() != null),
    mirrored("withRetry", c -> c.maxRetries() > 0),
    mirrored("withBackpressure", c -> c.backpressureHigh() != null),
    mirrored("withDeadLetterTopic", c -> c.deadLetterTopic() != null),
    mirrored("withErrorHandler", c -> c.errorHandler() != null),
    mirrored("withPollTimeout", c -> c.pollTimeout() != null)
  );

  /// Descriptor for the common case: a setting whose rejection message points at the
  /// `MultiBuilder` mirror method of the same name.
  private static ConsumerWideSetting mirrored(final String setting, final Predicate<ConsumerConfig> isSet) {
    final var mirror = "MultiBuilder.%s(...)".formatted(setting);
    return new ConsumerWideSetting(
      setting,
      isSet,
      (topic, c) ->
        "Route '%s' sets %s on its Stream, but %s is a consumer-wide setting; ".formatted(topic, setting, setting) +
        "set it on %s instead.".formatted(mirror)
    );
  }

  /// Mutable mirror of [ConsumerConfig]'s components used only inside [#with]. Never escapes the
  /// package.
  static final class Mut {

    int maxRetries;
    Duration retryBackoff;
    Long backpressureHigh;
    Long backpressureLow;
    ProcessingMode processingMode;
    int keyOrderedMaxKeys;
    ConsumerMetrics consumerMetrics;
    Consumer<KPipeConsumer.ProcessingError> errorHandler;
    String deadLetterTopic;
    Duration pollTimeout;
    Tracer tracer;
    CircuitBreakerController circuitBreaker;

    static Mut from(final ConsumerConfig c) {
      final var m = new Mut();
      m.maxRetries = c.maxRetries;
      m.retryBackoff = c.retryBackoff;
      m.backpressureHigh = c.backpressureHigh;
      m.backpressureLow = c.backpressureLow;
      m.processingMode = c.processingMode;
      m.keyOrderedMaxKeys = c.keyOrderedMaxKeys;
      m.consumerMetrics = c.consumerMetrics;
      m.errorHandler = c.errorHandler;
      m.deadLetterTopic = c.deadLetterTopic;
      m.pollTimeout = c.pollTimeout;
      m.tracer = c.tracer;
      m.circuitBreaker = c.circuitBreaker;
      return m;
    }

    ConsumerConfig build() {
      return new ConsumerConfig(
        maxRetries,
        retryBackoff,
        backpressureHigh,
        backpressureLow,
        processingMode,
        keyOrderedMaxKeys,
        consumerMetrics,
        errorHandler,
        deadLetterTopic,
        pollTimeout,
        tracer,
        circuitBreaker
      );
    }
  }
}
