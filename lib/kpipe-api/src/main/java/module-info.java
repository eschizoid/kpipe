/// KPipe API module — the user-facing entry point. Provides the fluent
/// [io.github.eschizoid.kpipe.KPipe] factory
/// and the [io.github.eschizoid.kpipe.Stream] / [io.github.eschizoid.kpipe.Sink] /
/// [io.github.eschizoid.kpipe.Handle] types.
///
/// Depends transitively on `kpipe-core`, `kpipe-consumer`, `kpipe-producer`, and the JSON / Avro /
/// Protobuf format modules so a single dependency on `kpipe-api` is enough to write the
/// 5-line "hello world" KPipe consumer.
module io.github.eschizoid.kpipe {
  requires transitive io.github.eschizoid.kpipe.core;
  requires transitive io.github.eschizoid.kpipe.consumer;
  requires transitive io.github.eschizoid.kpipe.producer;
  requires transitive io.github.eschizoid.kpipe.format.json;
  requires transitive io.github.eschizoid.kpipe.format.avro;
  requires transitive io.github.eschizoid.kpipe.format.protobuf;
  requires kafka.clients;
  requires org.apache.avro;
  requires com.google.protobuf;

  exports io.github.eschizoid.kpipe;
}
