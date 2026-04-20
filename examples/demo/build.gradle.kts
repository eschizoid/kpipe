import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "org.kpipe.demo.DemoApp",
    )
  }
}

description = "KPipe - Demo Application Combining JSON, Avro, and Protobuf Pipelines"

dependencies {
  implementation(rootProject.libs.dslJson)
  implementation(rootProject.libs.avro)
  implementation(rootProject.libs.protobufJava)
  implementation(rootProject.libs.protobufUtil)
  implementation(project(":lib:kpipe-producer"))

  runtimeOnly(rootProject.libs.opentelemetrySdk)
  runtimeOnly(rootProject.libs.opentelemetryExporterOtlp)
  runtimeOnly(rootProject.libs.opentelemetryAutoconfigure)
}
