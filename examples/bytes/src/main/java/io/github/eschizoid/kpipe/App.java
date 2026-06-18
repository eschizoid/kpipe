package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.config.AppConfig;
import io.github.eschizoid.kpipe.consumer.config.KafkaConsumerConfig;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.HexFormat;

/// Minimal raw-bytes consumer demonstrating the KPipe facade. Useful for proxying, raw archival,
/// or pre-format inspection where you want `byte[]` end-to-end with no SerDe inside the pipeline.
/// Each record is logged with its length and a short hex preview via a custom [MessageSink].
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());
  private static final int PREVIEW_BYTES = 16;

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    final MessageSink<byte[]> hexPreviewSink = payload ->
      LOGGER.log(Level.INFO, "bytes len={0} preview={1}", payload.length, hexPreview(payload));

    try (final var handle = KPipe.bytes(config.topic(), props).toCustom(hexPreviewSink).start()) {
      LOGGER.log(Level.INFO, "Bytes consumer started for topic {0}", config.topic());
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in bytes consumer", e);
      System.exit(1);
    }
  }

  private static String hexPreview(final byte[] payload) {
    final var limit = Math.min(payload.length, PREVIEW_BYTES);
    final var hex = HexFormat.ofDelimiter(" ").formatHex(payload, 0, limit);
    return payload.length > limit ? hex + " ..." : hex;
  }
}
