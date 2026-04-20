import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "org.kpipe.App",
    )
  }
}

description = "KPipe - Kafka Consumer Application Using Protobuf"

dependencies {
  implementation(rootProject.libs.protobufJava)
  implementation(rootProject.libs.protobufUtil)
}
