/// KPipe Protobuf format module — provides ProtobufFormat and ProtobufConsoleSink.
///
/// Add this module to your build only if your pipeline consumes or produces Protobuf messages.
module org.kpipe.format.protobuf {
  requires transitive org.kpipe.core;
  requires com.google.protobuf;
  requires com.google.protobuf.util;

  exports org.kpipe.format.protobuf;
}
