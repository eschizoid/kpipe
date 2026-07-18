plugins {
  `java-library`
  jacoco
}

description = "KPipe JSON format support — JsonFormat, JSON processors, and console sink"

dependencies {
  api(project(":lib:kpipe-core"))

  implementation(libs.fastjson2)

  testImplementation(platform(libs.junitBom))
  testImplementation(libs.junitJupiter)
  testRuntimeOnly(libs.junitPlatformLauncher)
  testImplementation(libs.mockitoCore)
  testImplementation(libs.mockitoJunitJupiter)
  testImplementation(libs.slf4jSimple)
}
