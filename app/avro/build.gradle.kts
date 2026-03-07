import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.jvm.tasks.Jar

tasks.named<ShadowJar>("shadowJar") {
    archiveClassifier.set("all")
    manifest {
        attributes(
            "Main-Class" to "org.kpipe.App"
        )
    }
}

tasks.named<Jar>("jar") {
    enabled = false
}

description = "KPipe - Kafka Consumer Application Using Avro"

dependencies {
    implementation("org.apache.avro:avro:1.12.1")
}
