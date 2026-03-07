package org.kpipe.sink;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

class CapturingHandler extends Handler {

  private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

  @Override
  public void publish(final LogRecord record) {
    if (record.getMessage() != null) {
      buffer.writeBytes(record.getMessage().getBytes(StandardCharsets.UTF_8));
    }
  }

  @Override
  public void flush() {}

  @Override
  public void close() {}

  @Override
  public String toString() {
    return buffer.toString(StandardCharsets.UTF_8);
  }
}
