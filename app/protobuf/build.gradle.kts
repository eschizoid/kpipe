import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

tasks.named<ShadowJar>("shadowJar") {
    manifest {
        attributes["Main-Class"] = "org.kpipe.Main"
    }
}

description = "KPipe - Kafka Consumer Application Using Protobuf"
