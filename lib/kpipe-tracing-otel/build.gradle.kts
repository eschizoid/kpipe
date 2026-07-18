plugins {
  `java-library`
  jacoco
}

description = "KPipe Tracing OTel - OpenTelemetry-backed implementation of the kpipe Tracer SPI"

dependencies {
  api(project(":lib:kpipe-producer"))
  api(libs.opentelemetryApi)
  implementation(libs.kafkaClients)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  testImplementation(libs.opentelemetrySdk)
  testImplementation(libs.opentelemetrySdkTesting)

  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)

  testImplementation(project(":lib:kpipe-api"))
  testImplementation(project(":lib:kpipe-consumer"))
  testImplementation(project(":lib:kpipe-format-json"))
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainersJunitJupiter)
  testImplementation(libs.testcontainersKafka)
}
