import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.artifacts.VersionCatalogsExtension

tasks.named<ShadowJar>("shadowJar") {
  archiveClassifier.set("all")
  manifest {
    attributes(
      "Main-Class" to "org.kpipe.App",
    )
  }
}

val libsCatalog = rootProject.extensions.getByType<VersionCatalogsExtension>().named("libs")

description = "KPipe - Kafka Consumer Application Using Avro"

dependencies {
  implementation(libsCatalog.findLibrary("avro").get())
}
