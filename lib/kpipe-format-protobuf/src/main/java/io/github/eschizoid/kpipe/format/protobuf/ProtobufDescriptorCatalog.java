package io.github.eschizoid.kpipe.format.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/// A keyed catalog of Protobuf message descriptors. Each catalog instance owns its own
/// descriptors — build one explicitly, look up the descriptor you need, then pass it to a fresh
/// `ProtobufFormat(descriptor)`.
///
/// ```java
/// final var catalog = new ProtobufDescriptorCatalog();
/// catalog.add("user", UserProto.getDescriptor());
/// catalog.add("order", OrderProto.getDescriptor());
///
/// final var userFormat = new ProtobufFormat(catalog.get("user"));
/// final var orderFormat = new ProtobufFormat(catalog.get("order"));
/// ```
///
/// If you already have a `Descriptor` in hand, skip the catalog and construct
/// `new ProtobufFormat(descriptor)` directly.
public final class ProtobufDescriptorCatalog {

  private final Map<String, Descriptor> descriptors = new ConcurrentHashMap<>();

  /// Creates an empty catalog.
  public ProtobufDescriptorCatalog() {}

  /// Adds a descriptor under `key`.
  ///
  /// @param key        the lookup key
  /// @param descriptor the Protobuf message descriptor
  /// @return this catalog for fluent chaining
  public ProtobufDescriptorCatalog add(final String key, final Descriptor descriptor) {
    Objects.requireNonNull(key, "key cannot be null");
    Objects.requireNonNull(descriptor, "descriptor cannot be null");
    descriptors.put(key, descriptor);
    return this;
  }

  /// Returns the [Descriptor] registered under `key`, or null if missing. Use [#find(String)] for
  /// an `Optional`-returning variant.
  ///
  /// @param key the lookup key
  /// @return the descriptor, or null if not registered
  public Descriptor get(final String key) {
    return descriptors.get(key);
  }

  /// Returns the [Descriptor] registered under `key`, wrapped in `Optional`.
  ///
  /// @param key the lookup key
  /// @return `Optional.of(descriptor)` if registered, `Optional.empty()` otherwise
  public Optional<Descriptor> find(final String key) {
    return Optional.ofNullable(descriptors.get(key));
  }

  /// Returns an unmodifiable view of every descriptor in the catalog.
  ///
  /// @return key → descriptor view
  public Map<String, Descriptor> all() {
    return Collections.unmodifiableMap(descriptors);
  }

  /// Removes every descriptor from the catalog.
  public void clear() {
    descriptors.clear();
  }
}
