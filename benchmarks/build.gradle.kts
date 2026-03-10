plugins {
    java
    id("me.champeau.jmh") version "0.7.3"
}

val libsCatalog = rootProject.extensions.getByType<VersionCatalogsExtension>().named("libs")

dependencies {
    // Benchmarks consume public API from :lib
    implementation(project(":lib"))

    // Benchmark targets
    implementation(libsCatalog.findLibrary("kafkaClients").get())
    implementation(libsCatalog.findLibrary("parallelConsumerCore").get())
    implementation(libsCatalog.findLibrary("avro").get())
    implementation(libsCatalog.findLibrary("dslJson").get())

    // Logging for JMH forks
    implementation(libsCatalog.findLibrary("slf4jSimple").get())

    implementation(libsCatalog.findLibrary("kafkaTestCommonRuntime").get())

    implementation(libsCatalog.findLibrary("junitJupiterApi").get())
}

jmh {
    fun intProp(name: String, default: Int): Int {
        return providers.gradleProperty(name).orNull?.toIntOrNull() ?: default
    }

    fun stringProp(name: String, default: String): String {
        return providers.gradleProperty(name).orNull?.trim()?.takeIf { it.isNotEmpty() } ?: default
    }

    fun csvProp(name: String): List<String>? {
        return providers.gradleProperty(name).orNull?.split(',')?.map(String::trim)?.filter(String::isNotEmpty)
            ?.takeIf { it.isNotEmpty() }
    }

    warmupIterations = intProp("jmh.warmupIterations", 3)
    iterations = intProp("jmh.iterations", 5)
    fork = intProp("jmh.fork", 1)
    threads = intProp("jmh.threads", 1)

    csvProp("jmh.includes")?.let { includes = it }
    csvProp("jmh.profilers")?.let { profilers = it }

    val jmhResultFormat = stringProp("jmh.resultFormat", "TEXT")
    val jmhTmpDir = layout.buildDirectory.dir("tmp/jmh").get().asFile.absolutePath
    val jmhResultFile = layout.buildDirectory.file("results/jmh/results.${jmhResultFormat.lowercase()}").get().asFile

    benchmarkMode = listOf("thrpt")
    timeUnit = "s"
    failOnError = true
    forceGC = true
    resultFormat = jmhResultFormat
    resultsFile = jmhResultFile

    jvmArgs = listOf("-Djava.io.tmpdir=$jmhTmpDir")
}

tasks.withType<JavaCompile> {
    options.release.set(25)
}
