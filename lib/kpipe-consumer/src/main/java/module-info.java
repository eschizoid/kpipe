/// KPipe consumer module — Kafka consumer pipeline, processing, and sink infrastructure.
///
/// Format-specific runtimes (fastjson2, Avro, Protobuf) are provided by separate modules:
/// `kpipe-format-json`, `kpipe-format-avro`, `kpipe-format-protobuf`.
module io.github.eschizoid.kpipe.consumer {
  requires transitive io.github.eschizoid.kpipe.core;
  requires transitive io.github.eschizoid.kpipe.producer;
  requires jdk.httpserver;
  requires transitive kafka.clients;

  exports io.github.eschizoid.kpipe.consumer;
  exports io.github.eschizoid.kpipe.consumer.config;
  exports io.github.eschizoid.kpipe.consumer.metrics;
  exports io.github.eschizoid.kpipe.health;
}
