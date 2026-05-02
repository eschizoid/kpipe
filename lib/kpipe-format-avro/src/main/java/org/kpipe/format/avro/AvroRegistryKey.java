package org.kpipe.format.avro;

import org.apache.avro.generic.GenericRecord;
import org.kpipe.registry.RegistryKey;

/// Convenience factory for [RegistryKey] instances backed by Avro [GenericRecord].
///
/// Lives in `kpipe-format-avro` so the core consumer module doesn't pull in the Avro
/// runtime just to expose a typed key helper.
public final class AvroRegistryKey {

  private AvroRegistryKey() {}

  /// Creates a typed registry key for Avro `GenericRecord` operators and sinks.
  ///
  /// @param name the unique name of the registry entry
  /// @return a new RegistryKey<GenericRecord>
  public static RegistryKey<GenericRecord> of(final String name) {
    return RegistryKey.of(name, GenericRecord.class);
  }
}
