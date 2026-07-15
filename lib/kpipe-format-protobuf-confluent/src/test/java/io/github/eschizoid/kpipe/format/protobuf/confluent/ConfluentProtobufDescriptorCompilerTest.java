package io.github.eschizoid.kpipe.format.protobuf.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.format.protobuf.ProtobufDescriptorCompiler;
import io.github.eschizoid.kpipe.format.protobuf.ProtobufFormat;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/// Verifies the Confluent-backed compiler turns `.proto` text into a resolved protobuf descriptor,
/// including message-index resolution — running on the classpath, exactly how an SR user deploys.
class ConfluentProtobufDescriptorCompilerTest {

  private final ConfluentProtobufDescriptorCompiler compiler = new ConfluentProtobufDescriptorCompiler();

  @Test
  void compilesSingleMessageProtoToDescriptor() {
    final var proto = """
      syntax = "proto3";
      package com.kpipe.customer;
      message Customer { int64 id = 1; string name = 2; }
      """;

    final var descriptor = compiler.compile(proto, new int[] { 0 });

    assertEquals("Customer", descriptor.getName());
    assertEquals(2, descriptor.getFields().size());
    assertNotNull(descriptor.findFieldByName("id"));
    assertNotNull(descriptor.findFieldByName("name"));
  }

  @Test
  void messageIndexSelectsTheRightTopLevelMessage() {
    final var proto = """
      syntax = "proto3";
      message Order { int64 id = 1; }
      message Shipment { string tracking = 1; }
      """;

    assertEquals("Order", compiler.compile(proto, new int[] { 0 }).getName());
    assertEquals("Shipment", compiler.compile(proto, new int[] { 1 }).getName());
  }

  @Test
  void nestedMessageIndexWalksIntoNestedTypes() {
    final var proto = """
      syntax = "proto3";
      message Outer {
        int64 id = 1;
        message Inner { string tag = 1; }
      }
      """;

    final var inner = compiler.compile(proto, new int[] { 0, 0 });
    assertEquals("Inner", inner.getName());
    assertNotNull(inner.findFieldByName("tag"));
  }

  @Test
  void outOfRangeIndexIsRejected() {
    final var proto = "syntax = \"proto3\"; message A { int64 id = 1; }";
    assertThrows(IllegalArgumentException.class, () -> compiler.compile(proto, new int[] { 5 }));
    assertThrows(IllegalArgumentException.class, () -> compiler.compile(proto, new int[] {}));
  }

  @Test
  void serviceLoaderDiscoversTheConfluentCompiler() {
    final var found = ServiceLoader.load(ProtobufDescriptorCompiler.class).findFirst();
    assertTrue(found.isPresent(), "META-INF/services must register the Confluent compiler");
    assertInstanceOf(ConfluentProtobufDescriptorCompiler.class, found.get());
  }

  @Test
  void withRegistrySingleParamResolvesTheCompilerViaServiceLoader() {
    // The base's single-param API (mirroring AvroFormat.withRegistry) finds the impl on the path.
    final var format = ProtobufFormat.withRegistry(id -> "syntax=\"proto3\"; message X { int64 id = 1; }");
    assertNotNull(format);
  }
}
