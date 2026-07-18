plugins {
  `java-library`
  jacoco
}

description = "KPipe Metrics - telemetry interfaces for KPipe (no-op default; bring your own backend)"

dependencies {
  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)

  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)
}
