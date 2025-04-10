# ☕ KPipe - A Modern Kafka Consumer

A **modern, functional, and high-performance Kafka consumer** built using **Java 23** features like **virtual threads**,
**Scoped Values**, and **composable message processors**. Ideal for large-scale systems and shared library use across
multiple teams.

---

## 🚀 Why This Library?

### ✅ Modern Java 23 Features

- **Virtual Threads** for massive concurrency with minimal overhead.
- **ScopedValue** for clean, immutable trace propagation (Coming!)

### ♻️ Composable + Functional Design

- Message processors are **pure functions** (`Function<byte[], byte[]>`) and easily **composed** using
  `Function::andThen`.
- Teams can **register their own processors** via config or code.

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
  Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(configuredProcessors);
  
  // Create team-specific processing pipeline
  Function<byte[], byte[]> teamPipeline = MessageProcessorRegistry.pipeline(
    "parseJson",
    "validateSchema",
    "extractMetadata",
    "addTeamIdentifier"
  );
  
  // Apply conditional processing based on message contents
  Function<byte[], byte[]> smartPipeline = MessageProcessorRegistry.when(
    msg -> new String(msg).contains("priority"),
    MessageProcessorRegistry.get("highPriorityProcessor"),
    MessageProcessorRegistry.get("standardProcessor")
  );
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
  // Register a custom processor for uppercase conversion
  MessageProcessorRegistry.register("uppercase", jsonBytes ->
          new String(jsonBytes).toUpperCase().getBytes());
  
  // Register a processor that adds a custom field
          MessageProcessorRegistry.register("addEnvironment",
                                            DslJsonMessageProcessors.addField("environment", "production"));
  
  // Create a processor pipeline from registered processors
  Function<byte[], byte[]> pipeline = MessageProcessorRegistry.pipeline(
          "parseJson", "addEnvironment", "uppercase", "markProcessed"
  );
  
  // Add error handling to your pipeline
  Function<byte[], byte[]> safePipeline = MessageProcessorRegistry.withErrorHandling(
          pipeline,
          "{\"error\":\"Processing failed\"}".getBytes()
  );
  
  // Process messages with your pipeline
  byte[] result = safePipeline.apply(rawMessage);
  ```

---

## 🛠️ Requirements

- Java 23 (Preview/EA if not yet final)
- Apache Kafka 3.0+
- DslJson (via Maven or Gradle)

---

## 🥪 Running It

```bash
java KafkaConsumerApp
```

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
- [Reactive Design with Functional Java](https://www.baeldung.com/java-functional-programming)
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
