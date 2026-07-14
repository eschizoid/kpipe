import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `java-library`
  jacoco
  alias(libs.plugins.shadow)
}

description =
  "KPipe Protobuf Confluent Schema Registry support — ConfluentProtobufDescriptorCompiler " +
  "(runtime .proto compilation via Confluent ProtobufSchema)"

java {
  withSourcesJar()
  withJavadocJar()
  toolchain {
    languageVersion = JavaLanguageVersion.of(25)
  }
}

repositories {
  mavenCentral()
  maven { url = uri("https://packages.confluent.io/maven/") }
}

// This module compiles on the CLASSPATH, not the module path: Confluent's ProtobufSchema drags in
// Square Wire as two jars that both export `com.squareup.wire` (an illegal split-package on the
// module path). Shading + relocation collapses that split in the published jar; the module ships as
// an automatic module (stable Automatic-Module-Name below), like other shaded runtime-bundling libs.
dependencies {
  api(project(":lib:kpipe-format-protobuf"))
  api(project(":lib:kpipe-core"))
  implementation(libs.protobufJava)
  implementation(libs.kafkaProtobufProvider)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)
  // Gold-standard wire-compat check: Confluent's own serializer produces the bytes our parser reads.
  testImplementation("io.confluent:kafka-protobuf-serializer:8.0.0")
}

tasks.test {
  useJUnitPlatform()
}

// Bundle Confluent + Wire into one self-contained jar with conflict-prone packages relocated so a
// downstream consumer never sees the wire split-package (or clashes with its own copies). protobuf
// and the kpipe modules stay EXTERNAL — protobuf's Descriptor/Message are this module's API types.
tasks.named<ShadowJar>("shadowJar") {
  archiveClassifier.set("")
  mergeServiceFiles()

  val shadedBase = "io.github.eschizoid.kpipe.shaded"
  relocate("com.squareup.wire", "$shadedBase.wire")
  relocate("com.squareup.kotlinpoet", "$shadedBase.kotlinpoet")
  relocate("okio", "$shadedBase.okio")
  relocate("kotlin", "$shadedBase.kotlin")
  relocate("com.google.common", "$shadedBase.guava")
  relocate("com.google.errorprone", "$shadedBase.errorprone")
  relocate("com.google.j2objc", "$shadedBase.j2objc")
  relocate("com.google.thirdparty", "$shadedBase.guavathirdparty")
  relocate("com.google.gson", "$shadedBase.gson")
  relocate("com.palantir.javapoet", "$shadedBase.javapoet")
  relocate("com.fasterxml.jackson", "$shadedBase.jackson")
  relocate("org.yaml.snakeyaml", "$shadedBase.snakeyaml")
  relocate("org.apache.commons", "$shadedBase.commons")
  // NOT relocated on purpose: the generated protobuf common-types (com.google.api/type/rpc/...
  // from proto-google-common-protos) — protobuf registers descriptors by proto name, so relocating
  // the Java package risks descriptor-registration breakage; they're needed for schema references.
  // io.confluent.* is the feature's own code. Annotation-only jars (javax.annotation,
  // org.checkerframework, org.jetbrains) are compile-retention and don't clash at runtime.

  // Bundle ONLY the Confluent/Wire compiler. Everything the consumer already has as a proper
  // module stays EXTERNAL — bundling it would split those packages across this jar and the module.
  dependencies {
    exclude(project(":lib:kpipe-core"))
    exclude(project(":lib:kpipe-format-protobuf"))
    // protobuf-java(+util) stay EXTERNAL — Descriptor/Message are the shared API types.
    exclude(dependency("com.google.protobuf:protobuf-java"))
    exclude(dependency("com.google.protobuf:protobuf-java-util"))
    exclude(dependency("org.apache.kafka:kafka-clients"))
    exclude(dependency("org.apache.avro:avro"))
    exclude(dependency("io.swagger.core.v3:.*"))
    exclude(dependency("com.github.luben:zstd-jni"))
    exclude(dependency("org.lz4:lz4-java"))
    exclude(dependency("org.xerial.snappy:snappy-java"))
    // Dependency-free-core invariant: KPipe uses java.lang.System.Logger, never SLF4J. Don't ship
    // an SLF4J-API that would shadow/skew a consumer's own. Confluent's logging degrades to no-op.
    exclude(dependency("org.slf4j:.*"))
  }

  manifest {
    attributes("Automatic-Module-Name" to "io.github.eschizoid.kpipe.format.protobuf.confluent")
  }
}

// The published/consumed artifact is the shaded jar.
tasks.named("jar") { enabled = false }
tasks.named("assemble") { dependsOn(tasks.named("shadowJar")) }

configurations {
  // Replace the plain jar in the outgoing api/runtime variants with the shaded jar.
  apiElements.get().outgoing.artifacts.clear()
  runtimeElements.get().outgoing.artifacts.clear()
  apiElements.get().outgoing.artifact(tasks.named("shadowJar"))
  runtimeElements.get().outgoing.artifact(tasks.named("shadowJar"))
}

tasks.jacocoTestReport {
  reports {
    csv.required.set(true)
    xml.required.set(true)
    html.required.set(true)
  }
}
