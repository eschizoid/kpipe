/// This module defines the core components of the KPipe library, including
/// configuration, consumers, health checks, metrics, processors, registry,
/// and sinks.
module org.kpipe {
  requires com.fasterxml.jackson.core;
  requires java.net.http;
  requires jdk.httpserver;
  requires org.apache.avro;
  requires org.slf4j;
  requires dsl.json;
  requires kafka.clients;

  exports org.kpipe.config;
  exports org.kpipe.consumer;
  exports org.kpipe.consumer.enums;
  exports org.kpipe.health;
  exports org.kpipe.metrics;
  exports org.kpipe.processor;
  exports org.kpipe.registry;
  exports org.kpipe.sink;
}
