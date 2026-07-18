plugins {
  `java-library`
  jacoco
}

description = "KPipe Protobuf format support — ProtobufFormat, processors, and console sink"

dependencies {
  api(project(":lib:kpipe-core"))
  implementation(libs.protobufJava)
  implementation(libs.protobufUtil)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)
  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)
  testImplementation(libs.slf4jSimple)
}
