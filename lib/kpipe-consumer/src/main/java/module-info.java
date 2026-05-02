/// KPipe consumer module — Kafka consumer pipeline, processing, and sink infrastructure.
///
/// Format-specific runtimes (DSL-JSON, Avro, Protobuf) are provided by separate modules:
/// `kpipe-format-json`, `kpipe-format-avro`, `kpipe-format-protobuf`.
module org.kpipe.consumer {
  requires transitive org.kpipe.core;
  requires transitive org.kpipe.producer;
  requires java.net.http;
  requires jdk.httpserver;
  requires kafka.clients;

  exports org.kpipe.consumer;
  exports org.kpipe.consumer.config;
  exports org.kpipe.consumer.enums;
  exports org.kpipe.consumer.metrics;
  exports org.kpipe.health;
}
