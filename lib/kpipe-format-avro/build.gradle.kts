plugins {
  `java-library`
  jacoco
}

description = "KPipe Avro format support — AvroFormat, Avro processors, and console sink"

dependencies {
  api(project(":lib:kpipe-core"))
  implementation(libs.avro)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)
  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)
  testImplementation(libs.slf4jSimple)
}
