/// KPipe producer module — Kafka producer and Kafka-backed sink implementations.
module org.kpipe.producer {
  requires transitive org.kpipe.core;
  requires transitive org.kpipe.metrics;
  requires kafka.clients;

  exports org.kpipe.producer;
  exports org.kpipe.producer.config;
  exports org.kpipe.producer.sink;
}
