plugins {
  `java-library`
  jacoco
}

description = "KPipe test kit — TestStream driver and CapturingSink for Docker-free pipeline unit tests"

dependencies {
  api(project(":lib:kpipe-core"))
  api(project(":lib:kpipe-consumer"))

  implementation(libs.kafkaClients)

  testImplementation(project(":lib:kpipe-format-json"))
  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)
  testImplementation(libs.slf4jSimple)
}
