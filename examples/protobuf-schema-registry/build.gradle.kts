import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "io.github.eschizoid.kpipe.App",
    )
  }
}

description = "KPipe - Confluent Schema Registry Protobuf example"

dependencies {
  implementation(project(":lib:kpipe-format-protobuf"))
  // The Protobuf `.proto`-text compiler ships in a separate shaded module (protobuf-java has no
  // `.proto` parser), discovered at runtime via ServiceLoader — the registry path needs it present.
  runtimeOnly(project(":lib:kpipe-format-protobuf-confluent"))
  implementation(project(":lib:kpipe-schema-registry-confluent"))
  implementation(rootProject.libs.protobufJava)
}
