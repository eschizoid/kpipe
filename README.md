# <img src="img/kpipe.png" width="100" height="100">

# ☕ KPipe - A Modern Kafka Consumer

A **modern, functional, and high-performance Kafka consumer** built using **Java 23** features like **virtual threads**,
**composable message processors**, and **DslJson** for JSON processing. Ideal for large-scale systems and shared library
use across multiple teams.

---

## 🚀 Why This Library?

### ✅ Modern Java 23 Features

- **Virtual Threads** for massive concurrency with minimal overhead.
- **ScopedValue** for clean, immutable trace propagation (Coming!)

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
  consumer.withProcessor(pipeline).withRetry(3, Duration.ofSeconds(1)).start();
  ```

### ⚡ High-Performance JSON Handling

- Uses **DslJson**, one of the fastest JSON libraries for Java (zero-copy, low-GC).
- Avoids unnecessary stringification or object mapping overhead.

### 🔌 Plug & Play for Teams

- Built as a shared library that teams can use and extend.
- **Register your own processors** in code or via configuration:

  ```java
  // Register custom processors for your team's needs
  MessageProcessorRegistry.register("extractMetadata", message -> {
    // Custom extraction logic here
    return processedMessage;
  });
  
  // Load processors from configuration
  String[] configuredProcessors = config.getStringArray("message.processors");
  Function<V, V> pipeline = MessageProcessorRegistry.pipeline(configuredProcessors);
  
  // Create a consumer with team-specific processing pipeline
  var consumer = new FunctionalKafkaConsumer.Builder<String, JsonObject>()
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
  
  // Apply conditional processing based on message contents
  Function<JsonObject, JsonObject> smartProcessor = msg -> {
      if (msg.containsKey("priority") && "high".equals(msg.get("priority"))) {
          return highPriorityProcessor.apply(msg);
      }
      return standardProcessor.apply(msg);
  };
  
  // Use the processor in your consumer
  consumer.start();
  ```

---

## 📆 Structure Overview

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
  MessageProcessorRegistry.register("uppercase", message -> {
      if (message instanceof JsonObject json) {
          if (json.containsKey("text")) {
              String text = json.getString("text");
              json.put("text", text.toUpperCase());
          }
          return json;
      }
      return message;
  });
  
  // Register a processor that adds environment information
  MessageProcessorRegistry.register("addEnvironment",
      DslJsonMessageProcessors.addField("environment", "production"));
  
  // Create a reusable processor pipeline
  var pipeline = MessageProcessorRegistry.pipeline(
      "parseJson", "validateSchema", "addEnvironment", "uppercase", "addTimestamp"
  );
  
  // Use the pipeline with a consumer
  var consumer = new FunctionalKafkaConsumer.Builder<String, JsonObject>()
      .withProperties(kafkaProps)
      .withTopic("events")
      .withProcessor(pipeline)
      .withRetry(3, Duration.ofSeconds(1))
      .build();
  
  // Start processing messages
  consumer.start();
  
  // Create a conditional processor for different message types
  Function<JsonObject, JsonObject> routingProcessor = msg -> {
      String type = msg.getString("type", "unknown");
      return switch(type) {
          case "order" -> orderProcessor.apply(msg);
          case "payment" -> paymentProcessor.apply(msg);
          default -> defaultProcessor.apply(msg);
      };
  };
  
  // Use the conditional processor
  consumer.withProcessor(routingProcessor).start();
  ```

---

## 🛠️ Requirements

- Java 23+
- Apache Kafka 3.0+
- DslJson (via Maven or Gradle)

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

Use it to modernize your Kafka stack with **Java 23 elegance and performance**.
