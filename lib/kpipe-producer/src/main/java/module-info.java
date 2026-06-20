/// KPipe producer module — Kafka producer and Kafka-backed sink implementations.
module io.github.eschizoid.kpipe.producer {
  requires transitive io.github.eschizoid.kpipe.core;
  requires transitive io.github.eschizoid.kpipe.metrics;
  requires transitive kafka.clients;

  exports io.github.eschizoid.kpipe.producer;
  exports io.github.eschizoid.kpipe.producer.config;
  exports io.github.eschizoid.kpipe.producer.sink;
  exports io.github.eschizoid.kpipe.producer.tracing;
}
