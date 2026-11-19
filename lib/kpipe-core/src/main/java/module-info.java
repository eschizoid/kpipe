/// KPipe core module — format-agnostic pipeline machinery: pipelines, operators, sink interface,
/// MessageFormat / Pipeline contracts.
///
/// Depends on no other KPipe module. Consumer, producer, and format modules all build on top of
/// these types.
module io.github.eschizoid.kpipe.core {
  exports io.github.eschizoid.kpipe.record;
  exports io.github.eschizoid.kpipe.pipeline;
  exports io.github.eschizoid.kpipe.sink;
}
