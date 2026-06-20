/// KPipe Protobuf format module — provides ProtobufFormat and ProtobufConsoleSink.
///
/// Add this module to your build only if your pipeline consumes or produces Protobuf messages.
module io.github.eschizoid.kpipe.format.protobuf {
  requires transitive io.github.eschizoid.kpipe.core;
  requires transitive com.google.protobuf;
  requires com.google.protobuf.util;

  exports io.github.eschizoid.kpipe.format.protobuf;
}
