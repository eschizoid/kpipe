plugins {
  `java-library`
  jacoco
}

description = "KPipe OTel - OpenTelemetry tracing and metrics for KPipe consumers"

dependencies {
  api(project(":lib:kpipe-consumer"))
  api(libs.opentelemetryApi)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  // Real SDK with in-memory exporters: tests read back each span and metric.
  testImplementation(libs.opentelemetrySdk)
  testImplementation(libs.opentelemetrySdkTesting)

  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)

  testImplementation(project(":lib:kpipe-api"))
  testImplementation(project(":lib:kpipe-format-json"))
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainersJunitJupiter)
  testImplementation(libs.testcontainersKafka)
}
