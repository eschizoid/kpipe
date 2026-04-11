module org.kpipe.producer {
  requires kafka.clients;

  exports org.kpipe.producer;
  exports org.kpipe.producer.config;
  exports org.kpipe.sink;
}
