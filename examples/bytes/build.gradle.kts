import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "io.github.eschizoid.kpipe.App",
    )
  }
}

description = "KPipe - Kafka Consumer Application Using Raw Bytes"

dependencies {
  // Docker-free integration test: real KPipeConsumer over a seeded MockConsumer, captured with
  // the kpipe-test CapturingSink.
  testImplementation(project(":lib:kpipe-test"))
}
