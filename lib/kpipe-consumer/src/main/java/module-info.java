module org.kpipe.consumer {
  requires transitive org.kpipe.producer;
  requires com.fasterxml.jackson.core;
  requires java.net.http;
  requires jdk.httpserver;
  requires dsl.json;
  requires kafka.clients;
  requires org.apache.avro;

  exports org.kpipe.consumer;
  exports org.kpipe.consumer.config;
  exports org.kpipe.consumer.enums;
  exports org.kpipe.consumer.sink;
  exports org.kpipe.health;
  exports org.kpipe.metrics;
  exports org.kpipe.processor;
  exports org.kpipe.registry;
}
