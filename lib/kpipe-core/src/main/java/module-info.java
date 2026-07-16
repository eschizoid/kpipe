/// KPipe core module — format-agnostic pipeline machinery: registries, processors, sink interface,
/// MessageFormat / MessagePipeline contracts.
///
/// Depends on no other KPipe module. Consumer, producer, and format modules all build on top of
/// these types.
module io.github.eschizoid.kpipe.core {
  exports io.github.eschizoid.kpipe.registry;
  exports io.github.eschizoid.kpipe.sink;
}
