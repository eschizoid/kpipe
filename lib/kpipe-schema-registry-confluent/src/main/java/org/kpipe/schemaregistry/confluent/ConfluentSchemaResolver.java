package org.kpipe.schemaregistry.confluent;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import org.kpipe.registry.SchemaResolver;

/// [SchemaResolver] implementation backed by Confluent Schema Registry's HTTP API.
///
/// Supports two lookup shapes:
///
///   * [#lookupById(int)] — `GET /schemas/ids/{id}`. The shape used during deserialization when
///     the schema id was extracted from the wire envelope. This is the [SchemaResolver] SPI
///     method.
///   * [#lookupBySubjectVersion(String, String)] — `GET /subjects/{subject}/versions/{version}`.
///     The shape used at startup to register a known schema before any record has arrived;
///     `version` can be a literal version number or the string `"latest"`.
///
/// Both methods return the unwrapped schema JSON — the response envelope `{"schema":"..."}` is
/// stripped before returning. The unwrap is performed inline with a tiny one-pass parser scoped
/// to the SR response shape so the module avoids a Jackson dependency.
///
/// **Caching is the caller's responsibility in v1.** Each lookup makes a network call. A future
/// version may add an internal LRU/TTL cache; today, wrap the resolver yourself if you want it.
///
/// **Thread-safety.** A single [ConfluentSchemaResolver] is safe to share across threads. The
/// underlying [HttpClient] is created once per instance and reused; close the resolver via
/// [#close()] when you're done to release its connection pool.
public final class ConfluentSchemaResolver implements SchemaResolver, AutoCloseable {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

  private final String baseUrl;
  private final HttpClient httpClient;
  private final Duration requestTimeout;

  /// Creates a resolver pointing at `baseUrl` (e.g. `http://schema-registry:8081`) with the
  /// default 10-second request timeout.
  ///
  /// @param baseUrl the SR base URL (must be non-blank; no trailing slash required)
  public ConfluentSchemaResolver(final String baseUrl) {
    this(baseUrl, DEFAULT_TIMEOUT);
  }

  /// Creates a resolver with an explicit request timeout. The same value is used for connect and
  /// request timeouts.
  ///
  /// @param baseUrl the SR base URL
  /// @param requestTimeout request and connect timeout (must be non-null)
  public ConfluentSchemaResolver(final String baseUrl, final Duration requestTimeout) {
    Objects.requireNonNull(baseUrl, "baseUrl cannot be null");
    if (baseUrl.isBlank()) throw new IllegalArgumentException("baseUrl cannot be blank");
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout cannot be null");
    this.httpClient = HttpClient.newBuilder().connectTimeout(requestTimeout).build();
  }

  @Override
  public String lookupById(final int schemaId) {
    if (schemaId < 0) throw new IllegalArgumentException("schemaId must be non-negative, got " + schemaId);
    return fetchAndUnwrap("%s/schemas/ids/%d".formatted(baseUrl, schemaId));
  }

  /// Fetches a schema by subject and version. `version` may be a literal version number (e.g.
  /// `"1"`) or `"latest"`.
  ///
  /// @param subject the SR subject name
  /// @param version the version identifier (or `"latest"`)
  /// @return the unwrapped schema JSON
  /// @throws RuntimeException on network failure, non-2xx response, or unparseable response body
  public String lookupBySubjectVersion(final String subject, final String version) {
    Objects.requireNonNull(subject, "subject cannot be null");
    Objects.requireNonNull(version, "version cannot be null");
    if (subject.isBlank()) throw new IllegalArgumentException("subject cannot be blank");
    if (version.isBlank()) throw new IllegalArgumentException("version cannot be blank");
    final var url = "%s/subjects/%s/versions/%s".formatted(
        baseUrl,
        URLEncoder.encode(subject, StandardCharsets.UTF_8),
        URLEncoder.encode(version, StandardCharsets.UTF_8)
      );
    return fetchAndUnwrap(url);
  }

  private String fetchAndUnwrap(final String url) {
    final var request = HttpRequest.newBuilder().uri(URI.create(url)).timeout(requestTimeout).GET().build();
    final HttpResponse<String> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (final IOException e) {
      throw new RuntimeException("Schema registry request failed: " + url, e);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Schema registry request interrupted: " + url, e);
    }

    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new RuntimeException("Schema registry returned HTTP %d for %s".formatted(response.statusCode(), url));
    }

    final var body = response.body();
    if (body == null || body.isEmpty()) throw new RuntimeException("Empty schema registry response for " + url);

    return unwrapSchemaField(body, url);
  }

  /// Extracts the value of the `"schema"` field from a Confluent SR JSON envelope, decoding any
  /// `\"` / `\\` escape sequences in the value.
  ///
  /// **Why hand-rolled, not Jackson.** The SR envelope is fixed-shape — one top-level object with
  /// a `"schema"` field whose value is a JSON-string-encoded payload. A two-state walker (find
  /// the field name, then read the string literal) handles every case the SR returns without
  /// pulling in Jackson on this module's classpath.
  private static String unwrapSchemaField(final String body, final String url) {
    final var marker = "\"schema\"";
    int i = body.indexOf(marker);
    if (i < 0) throw new RuntimeException("Schema field not found in response from " + url + ": " + truncate(body));
    i += marker.length();
    // Skip whitespace and the colon.
    while (i < body.length() && Character.isWhitespace(body.charAt(i))) i++;
    if (i >= body.length() || body.charAt(i) != ':') {
      throw new RuntimeException("Malformed schema field in response from " + url + ": " + truncate(body));
    }
    i++;
    while (i < body.length() && Character.isWhitespace(body.charAt(i))) i++;
    if (i >= body.length() || body.charAt(i) != '"') {
      throw new RuntimeException("Schema field is not a string in response from " + url + ": " + truncate(body));
    }
    i++; // opening quote

    final var sb = new StringBuilder(body.length() - i);
    while (i < body.length()) {
      final var c = body.charAt(i);
      if (c == '\\') {
        if (i + 1 >= body.length()) break;
        final var next = body.charAt(i + 1);
        switch (next) {
          case '"' -> sb.append('"');
          case '\\' -> sb.append('\\');
          case '/' -> sb.append('/');
          case 'n' -> sb.append('\n');
          case 't' -> sb.append('\t');
          case 'r' -> sb.append('\r');
          case 'b' -> sb.append('\b');
          case 'f' -> sb.append('\f');
          case 'u' -> {
            if (i + 5 >= body.length()) {
              throw new RuntimeException("Truncated unicode escape in schema field from " + url);
            }
            sb.append((char) Integer.parseInt(body, i + 2, i + 6, 16));
            i += 4;
          }
          default -> sb.append(next);
        }
        i += 2;
      } else if (c == '"') {
        return sb.toString();
      } else {
        sb.append(c);
        i++;
      }
    }
    throw new RuntimeException("Unterminated schema string in response from " + url + ": " + truncate(body));
  }

  private static String truncate(final String body) {
    return body.length() <= 200 ? body : body.substring(0, 200) + "...";
  }

  @Override
  public void close() {
    httpClient.close();
  }
}
