plugins {
  `java-platform`
}

description = "KPipe BOM — Maven Bill-of-Materials that pins all kpipe-* modules to matching versions"

dependencies {
  constraints {
    api(project(":lib:kpipe-core"))
    api(project(":lib:kpipe-metrics"))
    api(project(":lib:kpipe-metrics-otel"))
    api(project(":lib:kpipe-consumer"))
    api(project(":lib:kpipe-producer"))
    api(project(":lib:kpipe-format-json"))
    api(project(":lib:kpipe-format-avro"))
    api(project(":lib:kpipe-format-protobuf"))
    api(project(":lib:kpipe-api"))
    api(project(":lib:kpipe-test"))
  }
}
