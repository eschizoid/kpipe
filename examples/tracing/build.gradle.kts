import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "io.github.eschizoid.kpipe.App",
    )
  }
}

description = "KPipe - W3C trace context propagation example"

dependencies {
  implementation(project(":lib:kpipe-format-json"))
  implementation(project(":lib:kpipe-tracing-otel"))
  implementation(project(":lib:kpipe-producer"))
  implementation(rootProject.libs.opentelemetryApi)
  implementation(rootProject.libs.opentelemetrySdk)
  implementation(rootProject.libs.opentelemetryAutoconfigure)

  testImplementation(rootProject.libs.opentelemetrySdkTesting)
}
