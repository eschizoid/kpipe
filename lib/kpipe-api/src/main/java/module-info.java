/// KPipe API module — the user-facing entry point. Provides the fluent [org.kpipe.KPipe] factory
/// and the [org.kpipe.Stream] / [org.kpipe.Sink] / [org.kpipe.Handle] types.
///
/// Depends transitively on `kpipe-core`, `kpipe-consumer`, `kpipe-producer`, and the JSON / Avro /
/// Protobuf format modules so a single dependency on `kpipe-api` is enough to write the
/// 5-line "hello world" KPipe consumer.
module org.kpipe {
  requires transitive org.kpipe.core;
  requires transitive org.kpipe.consumer;
  requires transitive org.kpipe.producer;
  requires transitive org.kpipe.format.json;
  requires transitive org.kpipe.format.avro;
  requires transitive org.kpipe.format.protobuf;
  requires kafka.clients;
  requires org.apache.avro;
  requires com.google.protobuf;

  exports org.kpipe;
}
