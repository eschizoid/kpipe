import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
    manifest {
        attributes["Main-Class"] = "org.kpipe.Main"
    }
}

description = "KPipe - Kafka Consumer Application Using Avro"

dependencies {
    implementation("org.apache.avro:avro:1.12.1")
}
