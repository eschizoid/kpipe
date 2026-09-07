# Running the Fray suite locally

Companion note to ADR 0001. That ADR records _why_ the concurrency suite is moving to Fray; this one records how to
run it on a machine Fray does not support natively.

## Why there is a problem

Fray drives the JVM with a native JVMTI agent, and its Gradle plugin only wires that agent in on the three platforms
the agent is published for:

```kotlin
val supportedOsArchitectures = listOf("linux-x8664", "windows-x8664", "macos-aarch64")
```

macOS on **x86_64** is not on that list, and it is this project's primary development machine. On an unsupported
platform the plugin prints `Fray JVMTI agent will not be added as a dependency` and the build continues, so
`:lib:kpipe-consumer:frayTest` runs the tests on an ordinary JDK with no scheduling control at all.

That run is not merely useless, it is misleading: `@FrayTest` methods are reported as **skipped**, and any launcher
driven test degrades into repeating racy code and hoping. Both outcomes are green. `FrayInstrumentationSelfCheckTest`
exists to turn that state red, so on macOS x86_64 the expected local result is a **failing** `frayTest` task with:

```
The JVM running the Fray suite is /path/to/jdk, which carries no IMPLEMENTOR=Fray stamp.
```

A green `frayTest` on macOS x86_64 would mean the self-check itself has stopped working.

## Running it for real, in a container

`linux/amd64` is a supported Fray platform, and on an x86_64 Mac a `linux/amd64` container runs natively — there is no
emulation and no speed penalty. That makes a container the local equivalent of the CI runner rather than a slow
approximation of it.

The one wrinkle is that this repository is often checked out as a **linked git worktree**, whose `.git` file points at
an absolute path inside the main clone. Mounting the repository at its own absolute path keeps that pointer valid, and
keeps the axion-release plugin (which reads the repository through jgit at configuration time) working:

```bash
REPO=/absolute/path/to/kpipe          # the MAIN clone, even when working in a worktree
WORK=$PWD                             # the checkout you want to build

docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  -v "$REPO:$REPO" \
  -v "$HOME/.gradle-fray:/gradle-home" \
  -e HOME=/tmp/fray-home \
  -e GRADLE_USER_HOME=/gradle-home \
  -e JDK25_HOME=/opt/java/openjdk \
  -w "$WORK" \
  eclipse-temurin:25-jdk \
  ./gradlew --no-daemon :lib:kpipe-consumer:frayTest
```

Notes on the flags:

- `--platform linux/amd64` is what selects the supported Fray platform. Without it, Docker on an Apple Silicon host
  picks `linux/arm64`, for which no agent is published, and the self-check fails exactly as it does on the host.
- `JDK25_HOME` points Fray's `jlink` step at the JDK already in the image. Without it Fray downloads its own Amazon
  Corretto 25 (~200 MB) into `build/fray` on the first run.
- The bind-mounted `GRADLE_USER_HOME` keeps the Gradle distribution and the resolved dependencies between runs. The
  jlinked Fray JDK itself is cached separately, under `build/fray/fray-java`, and is rebuilt only when the Fray version
  changes.
- `--user` keeps build outputs owned by the host user rather than root.

On an Apple Silicon Mac none of this is needed: `macos-aarch64` is supported, so `./gradlew :lib:kpipe-consumer:frayTest`
works directly on the host.

## What a real run looks like

Two signals distinguish an instrumented run from the no-op:

1. `FrayInstrumentationSelfCheckTest` passes. Both of its gates have to hold — the JVM is Fray's jlinked image, **and**
   the scheduler finds a planted lost update.
2. `KeyOrderedEvictTombstoneFrayTest` prints its window hit rate, for example:

   ```
   Fray reached the eviction-tombstone window in N of M schedules
   ```

   The un-instrumented control on macOS x86_64 reaches it in roughly 1 schedule in 10,000, which is the same
   found-by-luck rate the jcstress suite showed (78 hits in 23,427 runs). A run whose rate is in that neighbourhood is
   not being scheduled by Fray, whatever the plugin logged.

## Reproducing a failure

A failing schedule writes a report directory under `build/fray/fray-report`, containing `fray.log` and the recording
files needed to replay that exact schedule. The CI job uploads it as an artifact on failure, because the schedule is
not recoverable from the test output alone.
