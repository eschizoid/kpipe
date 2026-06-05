/// KPipe JSON format module — provides JsonFormat and JsonConsoleSink.
///
/// Add this module to your build only if your pipeline consumes or produces JSON messages.
module io.github.eschizoid.kpipe.format.json {
  requires transitive io.github.eschizoid.kpipe.core;
  requires com.alibaba.fastjson2;

  exports io.github.eschizoid.kpipe.format.json;
}
