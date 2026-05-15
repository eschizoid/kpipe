import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "org.kpipe.App",
    )
  }
}

description = "KPipe - Circuit breaker example"

dependencies {
  implementation(project(":lib:kpipe-format-json"))
}
