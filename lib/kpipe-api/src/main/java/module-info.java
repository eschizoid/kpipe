/// KPipe API module — the user-facing entry point. Provides the fluent
/// [io.github.eschizoid.kpipe.KPipe] factory
/// and the [io.github.eschizoid.kpipe.Stream] / [io.github.eschizoid.kpipe.Sink] /
/// [io.github.eschizoid.kpipe.Handle] types.
///
/// Depends transitively on `kpipe-core`, `kpipe-consumer`, and `kpipe-producer`. The JSON / Avro /
/// Protobuf format modules are **opt-in** (`requires static`): the facade compiles against all
/// three, but a consumer only needs the format it actually uses on its runtime path. Writing
/// `KPipe.avro(...)` means adding `kpipe-format-avro`; a JSON-only app never pulls avro/protobuf.
module io.github.eschizoid.kpipe {
  requires transitive io.github.eschizoid.kpipe.core;
  requires transitive io.github.eschizoid.kpipe.consumer;
  requires transitive io.github.eschizoid.kpipe.producer;
  requires static io.github.eschizoid.kpipe.format.json;
  requires static io.github.eschizoid.kpipe.format.avro;
  requires static io.github.eschizoid.kpipe.format.protobuf;
  requires static org.apache.avro;
  requires static com.google.protobuf;

  exports io.github.eschizoid.kpipe;
}
