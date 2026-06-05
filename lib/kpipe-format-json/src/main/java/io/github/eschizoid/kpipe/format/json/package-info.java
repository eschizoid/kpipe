/// JSON codec for KPipe, backed by fastjson2.
///
/// Provides `JsonFormat` — a [io.github.eschizoid.kpipe.registry.MessageFormat] implementation that
/// decodes
/// raw Kafka bytes into JSON nodes and re-encodes them — and `JsonConsoleSink`, a pretty-
/// printing sink useful for development and debugging. Add `kpipe-format-json` to your build
/// only when your pipeline consumes or produces JSON messages.
package io.github.eschizoid.kpipe.format.json;
