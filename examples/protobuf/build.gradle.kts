import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "io.github.eschizoid.kpipe.App",
    )
  }
}

description = "KPipe - Kafka Consumer Application Using Protobuf"

dependencies {
  implementation(project(":lib:kpipe-format-protobuf"))
  implementation(rootProject.libs.protobufJava)
  implementation(rootProject.libs.protobufUtil)
}
