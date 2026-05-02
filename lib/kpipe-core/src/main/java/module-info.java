/// KPipe core module — format-agnostic pipeline machinery: registries, processors, sink interface,
/// MessageFormat / MessagePipeline contracts.
///
/// Depends on no other KPipe module. Consumer, producer, and format modules all build on top of
/// these types.
module org.kpipe.core {
  exports org.kpipe.registry;
  exports org.kpipe.sink;
}
