package org.kpipe.format.protobuf;

import com.google.protobuf.Message;
import org.kpipe.registry.RegistryKey;

/// Convenience factory for [RegistryKey] instances backed by Protobuf [Message].
///
/// Lives in `kpipe-format-protobuf` so the core consumer module doesn't pull in the Protobuf
/// runtime just to expose a typed key helper.
public final class ProtobufRegistryKey {

  private ProtobufRegistryKey() {}

  /// Creates a typed registry key for Protobuf `Message` operators and sinks.
  ///
  /// @param name the unique name of the registry entry
  /// @return a new RegistryKey<Message>
  public static RegistryKey<Message> of(final String name) {
    return RegistryKey.of(name, Message.class);
  }
}
