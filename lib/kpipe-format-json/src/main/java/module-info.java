/// KPipe JSON format module — provides JsonFormat, JsonMessageProcessor, and JsonConsoleSink.
///
/// Add this module to your build only if your pipeline consumes or produces JSON messages.
module org.kpipe.format.json {
  requires transitive org.kpipe.consumer;
  requires dsl.json;

  exports org.kpipe.format.json;
}
