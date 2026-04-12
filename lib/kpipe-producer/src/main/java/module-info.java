module org.kpipe.producer {
  requires transitive org.kpipe.metrics;
  requires kafka.clients;

  exports org.kpipe.producer;
  exports org.kpipe.producer.config;
  exports org.kpipe.sink;
}
