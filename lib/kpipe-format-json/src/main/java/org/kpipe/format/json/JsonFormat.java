package org.kpipe.format.json;

import com.dslplatform.json.DslJson;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.kpipe.registry.MessageFormat;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.registry.RegistryKey;

/// JSON implementation of [MessageFormat] for KPipe.
///
/// Serializes / deserializes JSON payloads as `Map<String, Object>` via DSL-JSON. JSON is
/// schema-less here: there is no schema registration on the format. Users wanting typed payloads
/// should project the deserialized map to a domain object inside `.pipe(...)`.
///
/// Example:
/// ```java
/// final var jsonFormat = JsonFormat.INSTANCE;
/// byte[] bytes = jsonFormat.serialize(Map.of("id", "123", "name", "John"));
/// Map<String, Object> result = jsonFormat.deserialize(bytes);
/// ```
public final class JsonFormat implements MessageFormat<Map<String, Object>> {

  /// Shared singleton instance — JsonFormat is stateless, so there is no isolation concern.
  public static final JsonFormat INSTANCE = new JsonFormat();

  /// Pre-defined registry key for the bundled `JsonConsoleSink` (the default sink wired up by
  /// [#newRegistry()]).
  public static final RegistryKey<Map<String, Object>> JSON_LOGGING = RegistryKey.json("jsonLogging");

  private static final DslJson<Map<String, Object>> DSL_JSON = new DslJson<>();

  /// Constructs a new JsonFormat instance.
  public JsonFormat() {
    // Default constructor
  }

  /// Creates a new [MessageProcessorRegistry] pre-wired with [JsonFormat#INSTANCE] and a
  /// [JsonConsoleSink] registered under [JsonFormat#JSON_LOGGING].
  ///
  /// @return a new pre-configured registry
  public static MessageProcessorRegistry newRegistry() {
    final var registry = new MessageProcessorRegistry(INSTANCE);
    registry.register(JSON_LOGGING, new JsonConsoleSink<>());
    return registry;
  }

  /// Creates a new [JsonConsoleSink] for `Map<String, Object>` payloads.
  ///
  /// @return a new console sink
  public static JsonConsoleSink<Map<String, Object>> consoleSink() {
    return new JsonConsoleSink<>();
  }

  /// Serializes the given data to a byte array.
  ///
  /// @param data the data to serialize
  /// @return the serialized byte array
  @Override
  public byte[] serialize(final Map<String, Object> data) {
    if (data == null) return null;
    try (final var output = new ByteArrayOutputStream()) {
      DSL_JSON.serialize(data, output);
      return output.toByteArray();
    } catch (final Exception e) {
      throw new RuntimeException("Failed to serialize JSON", e);
    }
  }

  /// Deserializes the given byte array to a `Map<String, Object>`.
  ///
  /// @param data the serialized byte array
  /// @return the deserialized map
  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Object> deserialize(final byte[] data) {
    if (data == null || data.length == 0) return null;
    try (final var input = new ByteArrayInputStream(data)) {
      return (Map<String, Object>) DSL_JSON.deserialize(Map.class, input);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to deserialize JSON", e);
    }
  }
}
