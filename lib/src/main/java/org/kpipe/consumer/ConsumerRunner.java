package org.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.kpipe.metrics.MetricsReporter;

/**
 * A thread-safe runner for {@link FunctionalConsumer} instances that manages the consumer
 * lifecycle.
 *
 * <p>The ConsumerRunner provides:
 *
 * <ul>
 *   <li>Controlled startup and shutdown
 *   <li>Health monitoring
 *   <li>Graceful shutdown with in-flight message handling
 *   <li>Metrics reporting at configurable intervals
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * FunctionalConsumer<String, String> consumer = new FunctionalConsumer.Builder<>()
 *     .withTopic("my-topic")
 *     .withProcessor(message -> processMessage(message))
 *     .build();
 *
 * ConsumerRunner<FunctionalConsumer<String, String>> runner = ConsumerRunner.builder(consumer)
 *     .withHealthCheck(FunctionalConsumer::isRunning)
 *     .withShutdownHook(true)
 *     .withShutdownTimeout(5000)
 *     .build();
 *
 * runner.start();
 * runner.awaitShutdown();
 * }</pre>
 *
 * @param <T> the type of consumer being managed, must extend FunctionalConsumer
 */
public class ConsumerRunner<T extends FunctionalConsumer<?, ?>> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(ConsumerRunner.class.getName());

  // Consumer state
  private final T consumer;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);

  // Configuration
  private final Consumer<T> startAction;
  private final Predicate<T> healthCheck;
  private final BiFunction<T, Long, Boolean> gracefulShutdown;
  private final long shutdownTimeoutMs;

  // Metrics
  private final List<MetricsReporter> metricsReporters;
  private final long metricsInterval;
  private volatile Thread metricsThread;

  private ConsumerRunner(final Builder<T> builder) {
    this.consumer = builder.consumer;
    this.startAction = builder.startAction;
    this.healthCheck = builder.healthCheck;
    this.gracefulShutdown = builder.gracefulShutdown;
    this.shutdownTimeoutMs = builder.shutdownTimeout;
    this.metricsReporters = new ArrayList<>(builder.metricsReporters);
    this.metricsInterval = builder.metricsInterval;

    if (builder.useShutdownHook) {
      Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }
  }

  /**
   * Creates a new builder for configuring a ConsumerRunner.
   *
   * @param <T> the type of consumer to run
   * @param consumer the consumer instance to manage
   * @return a new builder instance
   */
  public static <T extends FunctionalConsumer<?, ?>> Builder<T> builder(final T consumer) {
    return new Builder<>(consumer);
  }

  /**
   * Starts the consumer if it hasn't been started already.
   *
   * <p>This method is idempotent - calling it multiple times has no effect after the first call.
   *
   * @throws RuntimeException if the consumer fails to start
   */
  public void start() {
    if (started.compareAndSet(false, true)) {
      final Runnable performStart = () -> {
        startAction.accept(consumer);
        startMetricsThread();
      };
      try {
        performStart.run();
      } catch (final Exception e) {
        started.set(false);
        LOGGER.log(Level.ERROR, "Failed to start consumer", e);
        throw e;
      }
    }
  }

  /**
   * Checks if the consumer is healthy, according to the configured health check.
   *
   * <p>This method uses a functional approach by composing predicates to determine health status.
   * It checks three conditions:
   *
   * <ol>
   *   <li>The consumer is not closed
   *   <li>The consumer has been started
   *   <li>The consumer passes the configured health check
   * </ol>
   *
   * @return true if the consumer is healthy, false otherwise
   */
  public boolean isHealthy() {
    final Predicate<T> isNotClosed = c -> !closed.get();
    final Predicate<T> isStarted = c -> started.get();
    final Predicate<T> isHealthyPredicate = isNotClosed.and(isStarted).and(healthCheck);
    return isHealthyPredicate.test(consumer);
  }

  /**
   * Initiates a graceful shutdown of the consumer, waiting for in-flight messages to complete.
   *
   * @param timeoutMs maximum time in milliseconds to wait for in-flight messages
   * @return true if shutdown completed successfully, false if it timed out
   */
  public boolean shutdownGracefully(final long timeoutMs) {
    final Function<T, Boolean> performShutdown = c -> {
      stopMetricsThread();
      try {
        return gracefulShutdown.apply(c, timeoutMs);
      } finally {
        shutdownLatch.countDown();
      }
    };

    // Only perform shutdown if not already closed
    return closed.compareAndSet(false, true) ? performShutdown.apply(consumer) : true;
  }

  /**
   * Waits for the consumer to be shutdown, either by this thread or another thread calling {@link
   * #close()} or {@link #shutdownGracefully(long)}.
   *
   * <p>This is a convenience method that delegates to {@link #awaitShutdown(long)} with a timeout
   * of 0, meaning it will wait indefinitely.
   *
   * @return true if the shutdown completed normally, false if the wait was interrupted
   */
  public boolean awaitShutdown() {
    return awaitShutdown(0);
  }

  /**
   * Waits up to the specified timeout for the consumer to be shutdown.
   *
   * @param timeoutMs maximum time in milliseconds to wait, 0 means wait indefinitely
   * @return true if the shutdown completed normally within the timeout, false otherwise
   */
  public boolean awaitShutdown(final long timeoutMs) {
    final Function<Long, Boolean> waitOnLatch = timeout -> {
      try {
        return timeout > 0
          ? shutdownLatch.await(timeout, TimeUnit.MILLISECONDS)
          : shutdownLatch.await(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    };
    return waitOnLatch.apply(timeoutMs);
  }

  /**
   * Closes this consumer runner, performing a graceful shutdown.
   *
   * <p>This implementation delegates to {@link #shutdownGracefully(long)} with the configured
   * timeout. It follows functional programming principles by using a declarative approach.
   */
  @Override
  public void close() {
    shutdownGracefully(shutdownTimeoutMs);
  }

  /**
   * Performs a graceful shutdown of a FunctionalConsumer by handling in-flight messages.
   *
   * <p>This method follows these steps to ensure a clean shutdown:
   *
   * <ol>
   *   <li>Pauses the consumer to prevent receiving new messages
   *   <li>Obtains a message tracker from the consumer
   *   <li>Checks for in-flight messages:
   *       <ul>
   *         <li>If no in-flight messages exist, closes the consumer immediately
   *         <li>If in-flight messages exist, waits for their completion (up to timeout)
   *       </ul>
   *   <li>Verifies if all messages were processed
   *   <li>Closes the consumer regardless of processing outcome
   * </ol>
   *
   * <p>The method calls {@code getInFlightMessageCount()} twice - first to determine if any
   * messages need processing, and then after the wait period to confirm the final state.
   *
   * @param <T> the type of consumer extending FunctionalConsumer
   * @param consumer the consumer to shut down
   * @param timeoutMs maximum time in milliseconds to wait for in-flight messages to complete
   * @return {@code true} if all in-flight messages were successfully processed before shutdown,
   *     {@code false} if the timeout was reached with messages still in-flight
   * @throws RuntimeException if an exception occurs during the shutdown process
   */
  public static <T extends FunctionalConsumer<?, ?>> boolean performGracefulConsumerShutdown(
    final T consumer,
    final long timeoutMs
  ) {
    // Pause the consumer first to prevent receiving new messages
    consumer.pause();

    return Optional
      .ofNullable(consumer.createMessageTracker())
      .map(tracker -> {
        try {
          // First check for in-flight messages
          long inFlightCount = tracker.getInFlightMessageCount();

          if (inFlightCount == 0) {
            // No in-flight messages, close immediately
            consumer.close();
            return true;
          }

          // Log and wait for in-flight messages
          LOGGER.log(Level.INFO, "Waiting for %s in-flight messages to be processed".formatted(inFlightCount));

          var completed = false;
          try {
            completed = tracker.waitForCompletion(timeoutMs).orElse(false);
          } catch (final Exception e) {
            LOGGER.log(Level.WARNING, "Error while waiting for message completion", e);
          }

          // Second check to determine the final state
          inFlightCount = tracker.getInFlightMessageCount();
          final var allProcessed = completed && inFlightCount == 0;

          if (allProcessed) {
            LOGGER.log(Level.INFO, "All in-flight messages processed, shutting down");
          } else {
            LOGGER.log(
              Level.WARNING,
              "Shutdown timeout reached with %s messages still in flight".formatted(inFlightCount)
            );
          }

          consumer.close();
          return allProcessed;
        } catch (final Exception e) {
          LOGGER.log(Level.WARNING, "Error during graceful shutdown", e);
          consumer.close();
          return false;
        }
      })
      .orElseGet(() -> {
        // No message tracker available
        LOGGER.log(Level.WARNING, "No message tracker available, closing consumer immediately");
        consumer.close();
        return true;
      });
  }

  /**
   * Starts a metrics reporting thread if metrics reporters are configured. Uses functional
   * programming to handle the metrics reporting loop.
   */
  private void startMetricsThread() {
    if (metricsReporters.isEmpty() || metricsInterval <= 0) {
      return;
    }

    // Function to report metrics for all reporters
    final Runnable reportAllMetrics = () ->
      metricsReporters.forEach(reporter -> {
        try {
          reporter.reportMetrics();
        } catch (Exception e) {
          LOGGER.log(Level.WARNING, "Error reporting metrics", e);
        }
      });

    // Predicate to check if the thread should continue running
    final Predicate<Thread> shouldContinue = thread -> !closed.get() && !thread.isInterrupted();

    metricsThread =
      Thread
        .ofPlatform()
        .name("metrics-reporter")
        .daemon(true)
        .start(() -> {
          final var currentThread = Thread.currentThread();
          while (shouldContinue.test(currentThread)) {
            try {
              reportAllMetrics.run();
              Thread.sleep(metricsInterval);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        });
  }

  /**
   * Stops the metrics reporting thread if it's running. Uses Optional to handle the thread
   * reference in a functional way.
   */
  private void stopMetricsThread() {
    Optional
      .ofNullable(metricsThread)
      .ifPresent(thread -> {
        thread.interrupt();
        try {
          thread.join(1000); // Wait up to 1 second for thread to terminate
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.log(Level.WARNING, "Interrupted while stopping metrics thread");
        } finally {
          metricsThread = null;
        }
      });
  }

  /**
   * Builder for creating ConsumerRunner instances with custom configuration.
   *
   * @param <T> the type of consumer being managed
   */
  public static class Builder<T extends FunctionalConsumer<?, ?>> {

    private final T consumer;
    private Consumer<T> startAction;
    private Predicate<T> healthCheck;
    private BiFunction<T, Long, Boolean> gracefulShutdown;
    private long shutdownTimeout = 30000;
    private boolean useShutdownHook = false;
    private final List<MetricsReporter> metricsReporters = new ArrayList<>();
    private long metricsInterval = 60000;

    private Builder(final T consumer) {
      this.consumer = consumer;
      this.startAction = T::start;
      this.healthCheck = c -> true;
      this.gracefulShutdown = ConsumerRunner::performGracefulConsumerShutdown;
    }

    /**
     * Sets a custom action to perform when starting the consumer.
     *
     * @param startAction the action to execute on consumer start
     * @return this builder instance
     */
    public Builder<T> withStartAction(final Consumer<T> startAction) {
      this.startAction = startAction;
      return this;
    }

    /**
     * Sets a predicate that determines if the consumer is healthy.
     *
     * @param healthCheck the predicate to use for health checks
     * @return this builder instance
     */
    public Builder<T> withHealthCheck(final Predicate<T> healthCheck) {
      this.healthCheck = healthCheck;
      return this;
    }

    /**
     * Sets a custom function to handle graceful shutdown of the consumer.
     *
     * @param gracefulShutdown the function to use for graceful shutdown
     * @return this builder instance
     */
    public Builder<T> withGracefulShutdown(final BiFunction<T, Long, Boolean> gracefulShutdown) {
      this.gracefulShutdown = gracefulShutdown;
      return this;
    }

    /**
     * Sets the shutdown timeout in milliseconds.
     *
     * @param shutdownTimeout maximum time to wait during shutdown
     * @return this builder instance
     */
    public Builder<T> withShutdownTimeout(final long shutdownTimeout) {
      this.shutdownTimeout = shutdownTimeout;
      return this;
    }

    /**
     * Configures whether to register a JVM shutdown hook that calls close().
     *
     * @param useShutdownHook true to register a shutdown hook, false otherwise
     * @return this builder instance
     */
    public Builder<T> withShutdownHook(final boolean useShutdownHook) {
      this.useShutdownHook = useShutdownHook;
      return this;
    }

    /**
     * Adds multiple metrics reporters to run periodically.
     *
     * @param reporters the collection of metrics reporters to add
     * @return this builder instance
     */
    public Builder<T> withMetricsReporters(final Collection<MetricsReporter> reporters) {
      this.metricsReporters.addAll(reporters);
      return this;
    }

    /**
     * Sets the interval in milliseconds between metrics reports.
     *
     * @param metricsInterval the reporting interval in milliseconds
     * @return this builder instance
     */
    public Builder<T> withMetricsInterval(final long metricsInterval) {
      this.metricsInterval = metricsInterval;
      return this;
    }

    /**
     * Applies a custom configuration function to this builder.
     *
     * <p>This method allows for composing multiple configuration steps.
     *
     * @param configurer a function that applies configuration to this builder
     * @return this builder instance
     */
    public Builder<T> with(final Function<Builder<T>, Builder<T>> configurer) {
      return configurer.apply(this);
    }

    /**
     * Builds a new ConsumerRunner with the configured settings.
     *
     * @return a new ConsumerRunner instance
     */
    public ConsumerRunner<T> build() {
      return new ConsumerRunner<>(this);
    }
  }
}
