rootProject.name = "kpipe"

include("lib")
include("lib:kpipe-consumer")
include("lib:kpipe-producer")

include("examples")
include("examples:json")
include("examples:avro")
include("examples:protobuf")

include("benchmarks")
