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
 * A consumer runner that manages application lifecycle.
 *
 * <p>This class provides a reusable pattern for starting, monitoring, and gracefully shutting down
 * any service component. It follows functional programming principles by accepting behavior through
 * functional interfaces.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a runner for a Kafka consumer
 * FunctionalKafkaConsumer<String, String> consumer = new FunctionalKafkaConsumer.Builder<>()
 *     .withProperties(kafkaProps)
 *     .withTopic("my-topic")
 *     .withProcessor(message -> processMessage(message))
 *     .build();
 *
 * ConsumerRunner<FunctionalKafkaConsumer<String, String>> runner = ConsumerRunner
 *     .builder(consumer)
 *     .withStartAction(c -> {
 *         System.out.println("Starting consumer");
 *         c.start();
 *     })
 *     .withHealthCheck(FunctionalKafkaConsumer::isRunning)
 *     .withGracefulShutdown((c, timeout) -> {
 *         c.pause();
 *         long start = System.currentTimeMillis();
 *         while (System.currentTimeMillis() - start < timeout) {
 *             if (c.getMetrics().get("inFlightMessages") == 0) {
 *                 return true;
 *             }
 *             try {
 *                 Thread.sleep(100);
 *             } catch (InterruptedException e) {
 *                 Thread.currentThread().interrupt();
 *                 return false;
 *             }
 *         }
 *         return false;
 *     })
 *     .build();
 *
 * // Start the service
 * runner.start();
 *
 * // Register shutdown hook
 * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
 *     System.out.println("Shutting down gracefully");
 *     runner.shutdownGracefully(5000);
 * }));
 *
 * // Wait for termination
 * runner.awaitShutdown();
 * }</pre>
 *
 * @param <T> The type of service being managed (must implement AutoCloseable)
 */
public class ConsumerRunner<T extends AutoCloseable> implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(ConsumerRunner.class.getName());

  private final T service;
  private final Consumer<T> startAction;
  private final Function<T, Boolean> healthCheck;
  private final BiFunction<T, Long, Boolean> gracefulShutdown;
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private volatile boolean running = false;

  private ConsumerRunner(Builder<T> builder) {
    this.service = builder.service;
    this.startAction = builder.startAction;
    this.healthCheck = builder.healthCheck;
    this.gracefulShutdown = builder.gracefulShutdown;
  }

  /**
   * Starts the service by executing the configured start action.
   *
   * <p>If the service is already running, this method has no effect. The start action is defined
   * using {@link Builder#withStartAction(Consumer)}.
   */
  public void start() {
    if (running) return;
    running = true;
    startAction.accept(service);
  }

  /**
   * Checks if the service is healthy according to the configured health check.
   *
   * <p>The health check is defined using {@link Builder#withHealthCheck(Function)}.
   *
   * @return true if the service is running and the health check passes, false otherwise
   */
  public boolean isHealthy() {
    return running && healthCheck.apply(service);
  }

  /**
   * Attempts to shut down the service gracefully within the specified timeout.
   *
   * <p>This method executes the graceful shutdown function defined via {@link
   * Builder#withGracefulShutdown(BiFunction)}, then calls {@link #close()}.
   *
   * @param timeoutMs maximum time in milliseconds to wait for graceful shutdown
   * @return true if shutdown was successful, false otherwise
   */
  public boolean shutdownGracefully(final long timeoutMs) {
    if (!shuttingDown.compareAndSet(false, true)) return true;
    boolean result = gracefulShutdown.apply(service, timeoutMs);
    close();
    return result;
  }

  /**
   * Blocks the current thread until the service has been shut down.
   *
   * <p>This method is useful in main application threads to prevent the program from exiting while
   * the service is still running.
   *
   * @return true if shutdown completed normally, false if interrupted
   */
  public boolean awaitShutdown() {
    try {
      shutdownLatch.await();
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Shutdown wait interrupted");
      return false;
    }
  }

  /**
   * Closes the service and releases all resources.
   *
   * <p>This method is idempotent and can be called multiple times safely. It will close the
   * underlying service and signal completion to any threads waiting on {@link #awaitShutdown()}.
   */
  @Override
  public void close() {
    if (!running) return;
    running = false;
    try {
      service.close();
    } catch (Exception e) {
      LOGGER.log(Level.WARNING, "Error closing service", e);
    } finally {
      shutdownLatch.countDown();
    }
  }

  /**
   * Creates a new builder for constructing a ConsumerRunner.
   *
   * @param <T> the type of service to run
   * @param service the service instance to manage
   * @return a new builder instance
   * @throws NullPointerException if service is null
   */
  public static <T extends AutoCloseable> Builder<T> builder(final T service) {
    return new Builder<>(service);
  }

  public static class Builder<T extends AutoCloseable> {

    private final T service;
    private Consumer<T> startAction = t -> {};
    private Function<T, Boolean> healthCheck = t -> true;
    private BiFunction<T, Long, Boolean> gracefulShutdown = (t, timeout) -> true;

    private Builder(T service) {
      this.service = Objects.requireNonNull(service);
    }

    public Builder<T> withStartAction(final Consumer<T> startAction) {
      this.startAction = startAction;
      return this;
    }

    public Builder<T> withHealthCheck(final Function<T, Boolean> healthCheck) {
      this.healthCheck = healthCheck;
      return this;
    }

    public Builder<T> withGracefulShutdown(final BiFunction<T, Long, Boolean> gracefulShutdown) {
      this.gracefulShutdown = gracefulShutdown;
      return this;
    }

    public ConsumerRunner<T> build() {
      return new ConsumerRunner<>(this);
    }
  }
}
