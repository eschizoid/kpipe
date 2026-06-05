import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  archiveClassifier.set("all")
  manifest {
    attributes(
      "Main-Class" to "io.github.eschizoid.kpipe.App",
    )
  }
}

description = "KPipe - Kafka Consumer Application Using Avro"

dependencies {
  implementation(project(":lib:kpipe-format-avro"))
  implementation(project(":lib:kpipe-schema-registry-confluent"))
  implementation(libs.avro)
}
