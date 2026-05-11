rootProject.name = "kpipe"

include("lib")
include("lib:kpipe-bom")
include("lib:kpipe-core")
include("lib:kpipe-metrics")
include("lib:kpipe-metrics-otel")
include("lib:kpipe-tracing-otel")
include("lib:kpipe-consumer")
include("lib:kpipe-producer")
include("lib:kpipe-format-json")
include("lib:kpipe-format-avro")
include("lib:kpipe-format-protobuf")
include("lib:kpipe-api")

include("examples")
include("examples:json")
include("examples:avro")
include("examples:protobuf")
include("examples:demo")

include("benchmarks")
