/// KPipe facade module — provides the fluent `KPipe` entry point and `Stream`/`Sink`/`Handle`
/// types implementations for the common consumer path.
///
/// Depends transitively on `kpipe-core`, `kpipe-consumer`, `kpipe-producer`, and the JSON / Avro /
/// Protobuf format modules so a single dependency on `kpipe-facade` is enough to write the
/// 5-line "hello world" KPipe consumer.
module org.kpipe.facade {
  requires transitive org.kpipe.core;
  requires transitive org.kpipe.consumer;
  requires transitive org.kpipe.producer;
  requires transitive org.kpipe.format.json;
  requires transitive org.kpipe.format.avro;
  requires transitive org.kpipe.format.protobuf;
  requires kafka.clients;
  requires org.apache.avro;
  requires com.google.protobuf;

  exports org.kpipe.facade;
}
