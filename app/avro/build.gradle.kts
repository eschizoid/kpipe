description = "KPipe - Kafka Consumer Application Using Avro"

application {
    mainClass.set("org.kpipe.App")
}

dependencies {
    implementation("org.apache.avro:avro:1.12.0")
}
