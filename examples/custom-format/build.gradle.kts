import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "io.github.eschizoid.kpipe.App",
    )
  }
}

description = "KPipe - Kafka Consumer Application Using a User-Supplied MessageFormat"

// No format-module dependency: the MessageFormat<T> is implemented inline in App.java to
// demonstrate the user-supplied custom-format path through KPipe.custom(topic, props, format).

dependencies {
  // Docker-free integration test: real KPipeConsumer over a seeded MockConsumer, captured with
  // the kpipe-test CapturingSink. The test exercises the app's own CsvOrderFormat.
  testImplementation(project(":lib:kpipe-test"))
}
