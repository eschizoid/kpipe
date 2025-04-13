package org.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A runner that manages the lifecycle of FunctionalConsumer instances.
 *
 * <p>This class provides a reusable pattern for starting, monitoring, and gracefully shutting down
 * FunctionalConsumer components, with special handling for in-flight messages during shutdown.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Starts and monitors consumer instances
 *   <li>Provides health check mechanisms
 *   <li>Manages graceful shutdown with in-flight message handling
 *   <li>Offers configurable shutdown hooks and timeouts
 * </ul>
 *
 * <h2>Basic Usage Example</h2>
 *
 * <pre>{@code
 * // Create a consumer
 * FunctionalConsumer<String, String> consumer = new FunctionalConsumer.Builder<String, String>()
 *     .withTopic("my-topic")
 *     .withProcessor(msg -> msg.toUpperCase())
 *     .build();
 *
 * // Create and start a runner
 * ConsumerRunner<FunctionalConsumer<String, String>> runner = ConsumerRunner
 *     .builder(consumer)
 *     .withShutdownTimeout(5000)
 *     .withShutdownHook(true)
 *     .build();
 *
 * runner.start();
 *
 * // Wait for shutdown (triggered by JVM shutdown hook)
 * runner.awaitShutdown();
 * }</pre>
 *
 * <h2>Custom Graceful Shutdown Example</h2>
 *
 * <pre>{@code
 * ConsumerRunner<FunctionalConsumer<byte[], byte[]>> runner = ConsumerRunner
 *     .builder(consumer)
 *     .withGracefulShutdown((consumer, timeoutMs) -> {
 *         // Custom pre-shutdown actions
 *         someResource.close();
 *
 *         // Use the default shutdown behavior
 *         return ConsumerRunner.performGracefulConsumerShutdown(consumer, timeoutMs);
 *     })
 *     .build();
 * }</pre>
 *
 * <h2>Monitoring Example</h2>
 *
 * <pre>{@code
 * // Start a thread to monitor consumer health
 * Thread.startVirtualThread(() -> {
 *     while (!Thread.currentThread().isInterrupted()) {
 *         if (!runner.isHealthy()) {
 *             System.err.println("Consumer is unhealthy!");
 *         }
 *         Thread.sleep(Duration.ofSeconds(30));
 *     }
 * });
 * }</pre>
 */
public class ConsumerRunner<F extends FunctionalConsumer<?, ?>> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(ConsumerRunner.class.getName());

  private final F consumer;
  private final Consumer<F> startAction;
  private final Function<F, Boolean> healthCheck;
  private final BiFunction<F, Long, Boolean> gracefulShutdown;
  private final long shutdownTimeoutMs;
  private final boolean registerShutdownHook;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final AtomicBoolean running = new AtomicBoolean(false);
  private Thread shutdownHook;

  private ConsumerRunner(final Builder<F> builder) {
    this.consumer = builder.consumer;
    this.startAction = builder.startAction;
    this.healthCheck = builder.healthCheck;
    this.gracefulShutdown = builder.gracefulShutdown;
    this.shutdownTimeoutMs = builder.shutdownTimeoutMs;
    this.registerShutdownHook = builder.registerShutdownHook;
  }

  /**
   * Starts the consumer by executing the configured start action.
   *
   * <p>If the consumer is already running, this method has no effect. The start action is defined
   * using {@link Builder#withStartAction(Consumer)}.
   */
  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    try {
      startAction.accept(consumer);
    } catch (final Exception e) {
      running.set(false);
      LOGGER.log(Level.ERROR, "Error starting consumer", e);
      throw e;
    }

    // Register shutdown hook if enabled
    if (registerShutdownHook) {
      shutdownHook =
        new Thread(() -> {
          LOGGER.log(Level.INFO, "Executing shutdown hook");
          boolean success = shutdownGracefully(shutdownTimeoutMs);
          LOGGER.log(Level.INFO, "Shutdown hook completed with status: %s".formatted(success));
        });
      Runtime.getRuntime().addShutdownHook(shutdownHook);
      LOGGER.log(Level.INFO, "Consumer started with shutdown hook registered");
    } else {
      LOGGER.log(Level.INFO, "Consumer started (without shutdown hook)");
    }
  }

  /**
   * Checks if the consumer is healthy according to the configured health check.
   *
   * @return true if the consumer is running and the health check passes, false otherwise
   */
  public boolean isHealthy() {
    return running.get() && healthCheck.apply(consumer);
  }

  /**
   * Attempts to shut down the consumer gracefully within the specified timeout. This includes
   * pausing the consumer and waiting for in-flight messages to complete processing.
   *
   * @param timeoutMs maximum time in milliseconds to wait for graceful shutdown
   * @return true if shutdown completed successfully, false otherwise
   */
  public boolean shutdownGracefully(final long timeoutMs) {
    // If already shutting down, return true to indicate shutdown is in progress
    if (!shuttingDown.compareAndSet(false, true)) {
      LOGGER.log(Level.INFO, "Shutdown already in progress, ignoring redundant request");
      return true;
    }

    LOGGER.log(Level.INFO, "Beginning graceful shutdown with timeout: %s ms".formatted(timeoutMs));

    // If we have a registered shutdown hook, and we're not being called from it
    if (registerShutdownHook && shutdownHook != null && Thread.currentThread() != shutdownHook) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        LOGGER.log(Level.INFO, "Removed registered shutdown hook");
      } catch (final IllegalStateException e) {
        // JVM is already shutting down, ignore
      }
    }

    boolean completed = false;
    try {
      completed = gracefulShutdown.apply(consumer, timeoutMs);
      LOGGER.log(Level.INFO, "Graceful shutdown completed: %s".formatted(completed));
      return completed;
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error during graceful shutdown", e);
      return false;
    } finally {
      close();
    }
  }

  /**
   * Blocks the current thread until the consumer has been shut down.
   *
   * @return true if shutdown completed normally, false if interrupted
   */
  public boolean awaitShutdown() {
    try {
      LOGGER.log(Level.INFO, "Waiting for consumer shutdown");
      shutdownLatch.await();
      LOGGER.log(Level.INFO, "Consumer shutdown complete");
      return true;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Shutdown wait interrupted");
      return false;
    }
  }

  /**
   * Blocks the current thread until the consumer has been shut down or timeout occurs.
   *
   * @param timeoutMs maximum time in milliseconds to wait for shutdown
   * @return true if shutdown completed normally, false if interrupted or timed out
   */
  public boolean awaitShutdown(final long timeoutMs) {
    try {
      LOGGER.log(Level.INFO, "Waiting for consumer shutdown with timeout: %s ms".formatted(timeoutMs));
      boolean completed = shutdownLatch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
      if (completed) {
        LOGGER.log(Level.INFO, "Consumer shutdown complete");
      } else {
        LOGGER.log(Level.WARNING, "Consumer shutdown timed out after %s ms".formatted(timeoutMs));
      }
      return completed;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Shutdown wait interrupted");
      return false;
    }
  }

  /** Closes the consumer and releases all resources. */
  @Override
  public void close() {
    if (!running.compareAndSet(true, false)) return;

    try {
      LOGGER.log(Level.INFO, "Closing consumer");
      consumer.close();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error closing consumer", e);
    } finally {
      shutdownLatch.countDown();
    }
  }

  /**
   * Creates a new builder for constructing a ConsumerRunner.
   *
   * @param <F> the type of FunctionalConsumer to run
   * @param consumer the consumer instance to manage
   * @return a new builder instance
   */
  public static <F extends FunctionalConsumer<?, ?>> Builder<F> builder(final F consumer) {
    return new Builder<>(consumer);
  }

  public static class Builder<F extends FunctionalConsumer<?, ?>> {

    private final F consumer;
    private Consumer<F> startAction;
    private Function<F, Boolean> healthCheck = c -> c.isRunning();
    private BiFunction<F, Long, Boolean> gracefulShutdown;
    private long shutdownTimeoutMs = 10000; // Default 10 seconds
    private boolean registerShutdownHook = false; // Default to false so App can control it

    private Builder(final F consumer) {
      this.consumer = Objects.requireNonNull(consumer);

      // Set default start action
      this.startAction = F::start;

      // Set default graceful shutdown strategy
      this.gracefulShutdown = ConsumerRunner::performGracefulConsumerShutdown;
    }

    /**
     * Configures the action to execute when starting the consumer.
     *
     * @param startAction consumer that starts the consumer
     * @return this builder for chaining
     */
    public Builder<F> withStartAction(final Consumer<F> startAction) {
      this.startAction = startAction;
      return this;
    }

    /**
     * Configures the health check function for the consumer.
     *
     * @param healthCheck function that returns true if consumer is healthy
     * @return this builder for chaining
     */
    public Builder<F> withHealthCheck(final Function<F, Boolean> healthCheck) {
      this.healthCheck = healthCheck;
      return this;
    }

    /**
     * Configures the graceful shutdown strategy.
     *
     * @param gracefulShutdown function that implements graceful shutdown behavior
     * @return this builder for chaining
     */
    public Builder<F> withGracefulShutdown(final BiFunction<F, Long, Boolean> gracefulShutdown) {
      this.gracefulShutdown = gracefulShutdown;
      return this;
    }

    /**
     * Configures the shutdown timeout.
     *
     * @param shutdownTimeoutMs the timeout in milliseconds for graceful shutdown
     * @return this builder for chaining
     */
    public Builder<F> withShutdownTimeout(final long shutdownTimeoutMs) {
      this.shutdownTimeoutMs = shutdownTimeoutMs;
      return this;
    }

    /**
     * Configures whether to register a JVM shutdown hook.
     *
     * @param registerShutdownHook true to register a shutdown hook, false otherwise
     * @return this builder for chaining
     */
    public Builder<F> withShutdownHook(final boolean registerShutdownHook) {
      this.registerShutdownHook = registerShutdownHook;
      return this;
    }

    /**
     * Builds a new ConsumerRunner with the configured options.
     *
     * @return the configured ConsumerRunner
     */
    public ConsumerRunner<F> build() {
      return new ConsumerRunner<>(this);
    }
  }

  // Add this to ConsumerRunner.java
  public static <F extends FunctionalConsumer<?, ?>> boolean performGracefulConsumerShutdown(
    final F consumer,
    final long timeoutMs
  ) {
    try {
      consumer.pause();
      LOGGER.log(Level.INFO, "Consumer paused, waiting for in-flight messages...");

      final var tracker = consumer.createMessageTracker();
      final var inFlightBefore = tracker.getInFlightMessageCount();

      if (inFlightBefore > 0) {
        LOGGER.log(Level.INFO, "Waiting for %s in-flight messages".formatted(inFlightBefore));
        boolean completed = tracker.waitForCompletion(timeoutMs).orElse(false);

        long remaining = tracker.getInFlightMessageCount();
        LOGGER.log(Level.INFO, "Wait completed: %s, remaining messages: %s".formatted(completed, remaining));
        return completed && remaining == 0;
      }
      return true;
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error during graceful consumer shutdown", e);
      return false;
    }
  }
}
