import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
    archiveClassifier.set("all")
    manifest {
        attributes(
            "Main-Class" to "org.kpipe.App"
        )
    }
}

description = "KPipe - Kafka Consumer Application Using Avro"

dependencies {
    implementation("org.apache.avro:avro:1.12.1")
}
