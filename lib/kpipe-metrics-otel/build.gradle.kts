plugins {
  `java-library`
  jacoco
}

description = "KPipe Metrics OTel - OpenTelemetry-backed export of KPipe consumer metrics"

dependencies {
  api(project(":lib:kpipe-core"))
  api(project(":lib:kpipe-consumer"))
  api(libs.opentelemetryApi)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)

  // Real SDK + InMemoryMetricReader: tests read back each metric's name, value and attributes.
  testImplementation(libs.opentelemetrySdk)
  testImplementation(libs.opentelemetrySdkTesting)
}
