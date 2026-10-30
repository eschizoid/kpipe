import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "io.github.eschizoid.kpipe.globaltable.App",
    )
  }
}

description = "KPipe - Offsets in a DynamoDB global table for multi-region failover"

dependencies {
  implementation(rootProject.libs.awsDynamodb)
}
