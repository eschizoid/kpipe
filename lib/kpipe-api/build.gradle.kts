plugins {
  `java-library`
  jacoco
}

description = "KPipe API — fluent top-level KPipe entry point for the common consumer path"

dependencies {
  api(project(":lib:kpipe-core"))
  api(project(":lib:kpipe-consumer"))
  api(project(":lib:kpipe-producer"))

  // Formats are opt-in: the facade references them (KPipe.json/avro/protobuf, withSchemaRegistry)
  // but does not force them on consumers (`requires static` in module-info). A JSON-only consumer
  // adds only kpipe-format-json; it never pulls avro/protobuf at runtime.
  compileOnly(project(":lib:kpipe-format-json"))
  compileOnly(project(":lib:kpipe-format-avro"))
  compileOnly(project(":lib:kpipe-format-protobuf"))

  implementation(libs.kafkaClients)
  compileOnly(libs.avro)
  compileOnly(libs.protobufJava)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)
  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)
  testImplementation(libs.slf4jSimple)

  // kpipe-api's own tests exercise all three format facades, so it needs them (and the underlying
  // avro/protobuf libs, which are only compileOnly on the main path) at test scope.
  testImplementation(project(":lib:kpipe-format-json"))
  testImplementation(project(":lib:kpipe-format-avro"))
  testImplementation(project(":lib:kpipe-format-protobuf"))
  testImplementation(libs.avro)
  testImplementation(libs.protobufJava)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainersJunitJupiter)
  testImplementation(libs.testcontainersKafka)
}
