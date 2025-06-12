rootProject.name = "kpipe"

// Main modules
include("app")
include("lib")

// Submodules of app
include("app:json")
include("app:avro")
include("app:protobuf")
