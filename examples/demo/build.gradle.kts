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
  implementation(project(":lib:kpipe-api"))
  implementation(project(":lib:kpipe-consumer"))
  implementation(project(":lib:kpipe-format-json"))
  implementation(project(":lib:kpipe-format-avro"))
  implementation(project(":lib:kpipe-format-protobuf"))
  implementation(project(":lib:kpipe-schema-registry-confluent"))
  implementation(project(":lib:kpipe-producer"))
  implementation(project(":lib:kpipe-metrics-otel"))
  implementation(rootProject.libs.avro)
  implementation(rootProject.libs.protobufJava)
  implementation(rootProject.libs.protobufUtil)

  implementation(rootProject.libs.opentelemetrySdk)
  implementation(rootProject.libs.opentelemetryExporterOtlp)
  implementation(rootProject.libs.opentelemetryAutoconfigure)
}
