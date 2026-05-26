/// KPipe JSON format module — provides JsonFormat and JsonConsoleSink.
///
/// Add this module to your build only if your pipeline consumes or produces JSON messages.
module org.kpipe.format.json {
  requires transitive org.kpipe.core;
  requires com.alibaba.fastjson2;

  exports org.kpipe.format.json;
}
