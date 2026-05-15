import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "org.kpipe.App",
    )
  }
}

description = "KPipe - Confluent Schema Registry example"

dependencies {
  implementation(project(":lib:kpipe-format-avro"))
  implementation(project(":lib:kpipe-schema-registry-confluent"))
  implementation(rootProject.libs.avro)
}
