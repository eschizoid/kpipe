package io.github.eschizoid.kpipe.format.json;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONFactory;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import java.util.Map;

/// JSON implementation of [MessageFormat] for KPipe.
///
/// Serializes / deserializes JSON payloads as `Map<String, Object>` via fastjson2. JSON is
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

  static {
    // Explicit lockdown: disable autoType globally so a malicious payload cannot
    // instantiate arbitrary classes via the `@type` discriminator. fastjson2's default is
    // already off; we make it explicit to remove any configuration-drift risk.
    JSONFactory.getDefaultObjectReaderProvider().setAutoTypeBeforeHandler(null);
  }

  /// Constructs a new JsonFormat instance.
  public JsonFormat() {
    // Default constructor
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
    try {
      return JSON.toJSONBytes(data);
    } catch (final JSONException e) {
      throw new RuntimeException("Failed to serialize JSON", e);
    }
  }

  /// Deserializes the given byte array to a `Map<String, Object>`.
  ///
  /// @param data the serialized byte array
  /// @return the deserialized map
  @Override
  public Map<String, Object> deserialize(final byte[] data) {
    if (data == null || data.length == 0) return null;
    try {
      return JSON.parseObject(data);
    } catch (final JSONException e) {
      throw new RuntimeException("Failed to deserialize JSON", e);
    }
  }
}
