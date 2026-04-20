/// KPipe consumer module — Kafka consumer pipeline, processing, and sink infrastructure.
module org.kpipe.consumer {
  requires transitive org.kpipe.producer;
  requires com.fasterxml.jackson.core;
  requires java.net.http;
  requires jdk.httpserver;
  requires dsl.json;
  requires kafka.clients;
  requires org.apache.avro;
  requires com.google.protobuf;
  requires com.google.protobuf.util;

  exports org.kpipe.consumer;
  exports org.kpipe.consumer.config;
  exports org.kpipe.consumer.enums;
  exports org.kpipe.consumer.metrics;
  exports org.kpipe.consumer.sink;
  exports org.kpipe.health;
  exports org.kpipe.processor;
  exports org.kpipe.registry;
}
