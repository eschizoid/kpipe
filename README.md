# <img src="img/kpipe.png" width="200" height="200">

# ☕ KPipe - A Modern Kafka Consumer

A **modern, functional, and high-performance Kafka consumer** built using **Java 21** features like **virtual threads**,
**composable message processors**, and **DslJson** for JSON processing. Ideal for large-scale systems and shared library
use across multiple teams.

---

## 🚀 Why This Library?

### ✅ Modern Java 21 Features

- **Virtual Threads** for massive concurrency with minimal overhead.
- **ScopedValue** for clean, immutable trace propagation (Coming!)

### 🧩 Functional Processing Pipeline

- Message processors are **pure functions** (`Function<V, V>`) that transform data without side effects
- Build complex pipelines through **function composition** using `Function::andThen` or the registry
- **Declarative processing** lets you describe *what* to do, not *how* to do it
- **Higher-order functions** enable conditional processing, retry logic, and error handling
- Teams can **register their own processors** in a central registry via:
  
  ```java
  // Register team-specific processors
  MessageProcessorRegistry.register("sanitizeData",
      DslJsonMessageProcessors.removeFields("password", "ssn"));
  
  // Create pipelines from registered processors
  var pipeline = MessageProcessorRegistry.pipeline(
      "parseJson", "validateSchema", "sanitizeData", "addMetadata");
  
  // Apply transformations with built-in error handling and retry logic
  var consumer = new FunctionalKafkaConsumer.Builder<byte[], byte[]>()
    .withProcessor(pipeline)
    .withRetry(3, Duration.ofSeconds(1))
    .build();
  consumer.start();
  ```

  ```java
  // Register custom processors for your team's needs
  MessageProcessorRegistry.register("extractMetadata", message -> {
    // Custom extraction logic here
    return processedMessage;
  });
  
  // Load processors from configuration
  String[] configuredProcessors = config.getStringArray("message.processors");
  Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(configuredProcessors);
  
  // Create a consumer with team-specific processing pipeline
  var consumer = new FunctionalKafkaConsumer.Builder<byte[], byte[]>()
    .withProperties(kafkaProps)
    .withTopic("team-topic")
    .withProcessor(MessageProcessorRegistry.pipeline(
        "parseJson",
        "validateSchema",
        "extractMetadata",
        "addTeamIdentifier"))
  .withErrorHandler(error -> publishToErrorTopic(error))
  .withRetry(3, Duration.ofSeconds(1))
  .build();
  
  // Use the consumer
  consumer.start();
  ```

---

## 📆 Structure Overview (TODO Update this Section)

```
├── KafkaConsumerApp.java         # Main app with virtual thread runner
├── FunctionalKafkaConsumer.java  # Core Kafka wrapper using functional streams
├── KafkaConfigFactory.java       # Kafka properties factory
├── DslJsonMessageProcessors.java # High-perf JSON processors
└── MessageProcessorRegistry.java # Registry for dynamic processor loading
```

---

## ⚙️ Example: Add Custom Processor

Extend the registry like this:

  ```java
  // Register a processor for JSON field transformations
  MessageProcessorRegistry.register("uppercase", bytes ->
      DslJsonMessageProcessors.transformField("text", value -> {
          if (value instanceof String text) {
              return text.toUpperCase();
          }
          return value;
      }).apply(bytes)
  );
  
  // Register a processor that adds environment information
  MessageProcessorRegistry.register("addEnvironment",
      DslJsonMessageProcessors.addField("environment", "production"));
  
  // Create a reusable processor pipeline
  var pipeline = MessageProcessorRegistry.pipeline(
      "parseJson", "validateSchema", "addEnvironment", "uppercase", "addTimestamp"
  );
  
  // Use the pipeline with a consumer
  var consumer = new FunctionalKafkaConsumer.Builder<byte[], byte[]>()
      .withProperties(kafkaProps)
      .withTopic("events")
      .withProcessor(pipeline)
      .withRetry(3, Duration.ofSeconds(1))
      .build();
  
  // Start processing messages
  consumer.start();
  ```

---

## 📊 Built-in Metrics

Monitor your consumer with built-in metrics:

  ```java
  // Access consumer metrics
  Map<String, Long> metrics = consumer.getMetrics();
  System.out.println("Messages received: " + metrics.get("messagesReceived"));
  System.out.println("Successfully processed: " + metrics.get("messagesProcessed"));
  System.out.println("Processing errors: " + metrics.get("processingErrors"));
  ```

Configure automatic metrics reporting:

  ```java
  new KafkaConsumerApp(config)
    .withMetricsInterval(Duration.ofSeconds(30))
    .start();
  ```

---

## 🛡️ Graceful Shutdown
The consumer supports graceful shutdown with in-flight message handling:

  ```java
  // Initiate graceful shutdown with 5-second timeout
  boolean allProcessed = kafkaApp.shutdownGracefully(5000);
  if (allProcessed) {
    LOGGER.info("All messages processed successfully before shutdown");
  } else {
    LOGGER.warning("Shutdown completed with some messages still in flight");
  }
  
  // Register as JVM shutdown hook
  Runtime.getRuntime().addShutdownHook(
    new Thread(() -> app.shutdownGracefully(5000))
  );
  ```

---

## 🔧 Working with JSON

The JSON processors handle deserialization and transformation of JSON data:

  ```java
  // Add a timestamp field to messages
  var addTimestampProcessor = DslJsonMessageProcessors.addTimestamp("processedAt");
  
  // Remove sensitive fields
  var sanitizeProcessor = DslJsonMessageProcessors.removeFields("password", "ssn", "creditCard");
  
  // Transform specific fields
  var uppercaseSubjectProcessor = DslJsonMessageProcessors.transformField("subject", value -> {
      if (value instanceof String text) {
          return text.toUpperCase();
      }
      return value;
  });
  
  // Add metadata to messages
  Map<String, Object> metadata = new HashMap<>();
  metadata.put("version", "1.0");
  metadata.put("environment", "production");
  var addMetadataProcessor = DslJsonMessageProcessors.mergeWith(metadata);
  
  // Combine processors into a pipeline
  Function<byte[], byte[]> pipeline = message -> addMetadataProcessor.apply(
      uppercaseSubjectProcessor.apply(
          sanitizeProcessor.apply(
              addTimestampProcessor.apply(message)
          )
      )
  );
  
  // Or use the registry to build pipelines
  var registryPipeline = MessageProcessorRegistry.pipeline(
      "sanitize", "addTimestamp", "uppercaseSubject", "addMetadata"
  );
  ```

---

## 🛠️ Requirements

- Java 21+
- Apache Kafka 3.0+
- DslJson (via Maven or Gradle)

---

## ⚙️ Configuration

Configure via environment variables:

  ```bash
  export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  export KAFKA_CONSUMER_GROUP=my-consumer-group
  export KAFKA_TOPIC=json-events
  export KAFKA_PROCESSORS=parseJson,validateSchema,addTimestamp
  export METRICS_INTERVAL_MS=30000
  export SHUTDOWN_TIMEOUT_MS=5000
  ```

---

## 🥪 Running It

  ```bash
  java KafkaConsumerApp
  ```

TODO
- Add Dockerfile for easy deployment 

Kafka consumer will:

- Connect to `localhost:9092`
- Subscribe to `json-topic`
- Compose the processing pipeline from configured processors
- Process each message concurrently using virtual threads

---

## 🔍 Best Practices & Advanced Patterns

### Composing Complex Processing Pipelines

For maintainable pipelines, group related processors:

  ```java
  // Create focused processor groups
  var securityProcessors = MessageProcessorRegistry.pipeline(
      "sanitizeData", "encryptSensitiveFields", "addAuditTrail");
      
  var enrichmentProcessors = MessageProcessorRegistry.pipeline(
      "addMetadata", "addTimestamp", "addEnvironment");
      
  // Compose them into a master pipeline
  var fullPipeline = message -> enrichmentProcessors.apply(
      securityProcessors.apply(message));
  
  // Or register the composed pipeline
  MessageProcessorRegistry.register("standardProcessing", fullPipeline);
  ```

### Conditional Processing

The library provides a built-in `when()` method for conditional processing:

  ```java
  // Create a predicate that checks message type
  Predicate<byte[]> isOrderMessage = message -> {
      try {
          Map<String, Object> parsed = DslJsonMessageProcessors.parseJson().apply(message);
          return "ORDER".equals(parsed.get("type"));
      } catch (Exception e) {
          return false;
      }
  };
  
  // Use the built-in conditional processor
  Function<byte[], byte[]> conditionalProcessor = MessageProcessorRegistry.when(
      isOrderMessage,
      MessageProcessorRegistry.get("orderEnrichment"),
      MessageProcessorRegistry.get("defaultEnrichment")
  );
  
  // Register the conditional pipeline
  MessageProcessorRegistry.register("smartProcessing", conditionalProcessor);
  ```

### Thread-Safety Considerations
- Message processors should be stateless and thread-safe
- Avoid shared mutable state between processors
- Use immutable data structures where possible
- For processors with side effects (like database calls), consider using thread-local variables

### Performance Optimization
- Register frequently used processor combinations as single processors
- For very large messages, consider streaming JSON processors
- Profile your processor pipeline to identify bottlenecks

---

## 📈 Performance Notes

- Virtual threads are **1:1 with Kafka records** — scales to 100k+ messages/sec
- Zero-GC JSON processing
- Safe and efficient memory model using modern Java features

---

## 📚 Inspiration

This library is inspired by the best practices from:

- [Project Loom](https://openjdk.org/projects/loom/)
- [High-performance JSON libraries (DslJson, Jsoniter)](https://github.com/ngs-doo/dsl-json)

---

## 💬 Contributing

If you're a team using this library, feel free to:

- Register custom processors
- Add metrics/observability hooks
- Share improvements or retry strategies

---

## 🧠 Final Thoughts

This Kafka consumer is:

- **Functional**
- **Extensible**
- **Future-proof**

Use it to modernize your Kafka stack with **Java 21 elegance and performance**.
