import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
  manifest {
    attributes(
      "Main-Class" to "io.github.eschizoid.kpipe.offsetstore.GlobalTableApp",
    )
  }
}

description = "KPipe - Offset stores in Postgres and DynamoDB, with multi-region failover"

dependencies {
  implementation(rootProject.libs.awsDynamodb)
  implementation(rootProject.libs.postgresql)

  testImplementation(rootProject.libs.testcontainersPostgresql)
}
