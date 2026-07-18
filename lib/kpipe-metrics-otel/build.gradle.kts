plugins {
  `java-library`
  jacoco
}

description = "KPipe Metrics OTel - OpenTelemetry-backed implementation of kpipe-metrics interfaces"

dependencies {
  api(project(":lib:kpipe-metrics"))
  api(project(":lib:kpipe-core"))
  api(libs.opentelemetryApi)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)

  // Real SDK + InMemoryMetricReader for assertions that the OTel impls actually emit metrics
  // with the right name, value, and attributes — the previous noop-only smoke tests would let
  // emission regressions land silently.
  testImplementation(libs.opentelemetrySdk)
  testImplementation(libs.opentelemetrySdkTesting)
}
