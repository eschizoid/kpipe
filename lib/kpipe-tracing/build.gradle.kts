plugins {
  `java-library`
  jacoco
}

description = "KPipe Tracing - vendor-neutral Tracer SPI for cross-Kafka-boundary trace propagation (no-op default)"

dependencies {
  // The Tracer signatures expose ConsumerRecord and Headers, so kafka-clients is API surface.
  api(libs.kafkaClients)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)
}
