# ADR 0001 — Concurrency testing moves to Fray

- **Status:** Accepted
- **Date:** 2026-09-07
- **Supersedes:** nothing (first ADR in this repo)

## Context

kpipe gates every PR on a 21-class [jcstress](https://github.com/openjdk/jcstress) suite covering the offset
frontier, remove-if-empty, the backpressure handshake, dispatcher counters, and the key-ordered worker handoff and
eviction tombstone. The suite is the reason the at-least-once claim is testable rather than asserted.

jcstress is dormant. Its last release is **0.16 (2023-02-27)** — the version we pin — and its last upstream commit is
**2025-06-19**. It is not archived, and those final commits were JDK 25 fixes (native-access and `sun.misc.Unsafe`
warnings), but **those fixes exist only in git; no release carries them**. That raises a fair question: should the
suite move to a maintained tool?

Two candidates were evaluated by experiment rather than reputation.

## Decision

**Migrate the suite to [Fray](https://github.com/cmu-pasta/fray) and retire jcstress.**

Fray explores schedules under a controlled scheduler and replays failures deterministically, rather than running racy
code many times and hoping the bad interleaving surfaces. Our own numbers are the argument: the eviction-tombstone
window was reached **78 times out of 23,427 stress runs — 0.33%**. That window is currently found by luck, on every
run, forever. A scheduler-controlling tool targets it.

Fray is also the only maintained option: v0.9.0 released 2026-07-17, with CI covering JDK 11/21/25 on Linux, Windows
and macOS. jcstress has had no release since 0.16 (2023-02-27) and its JDK 25 fixes exist only in git.

Lincheck was evaluated and rejected — see below; it cannot instrument Java 25 class files at all.

## Evidence

Each tool was given the same probe: a deliberately racy counter (`counter++` from two threads, non-atomic) that a
working concurrency checker must reject.

### Lincheck 2.39 — silently passes on Java 25

| JDK | Result |
| --- | --- |
| 21 | **Correctly fails**, printing the exact interleaving (both threads read 0, both write 1) |
| 25 | **Passes** — false green |

Cause: Lincheck instruments bytecode with a bundled ASM that cannot read class file **major version 69** (Java 25).

```
java.lang.IllegalArgumentException: Unsupported class file major version 69
    at org.objectweb.asm.ClassReader.<init>
    at org.jetbrains.kotlinx.lincheck.transformation.LincheckClassFileTransformer
```

The transformer logs `Unable to transform`, continues, and the test passes. Lincheck itself is actively maintained
(2.39, 2025-04-09), so this may lift with a future release.

### Fray 0.9.0 — cannot run on this project's development machine

Fray is the most actively maintained of the three (v0.9.0 released 2026-07-17). It ships a **native JVMTI agent**,
and its Gradle plugin enumerates the platforms that agent exists for:

```kotlin
val supportedOsArchitectures = listOf("linux-x8664", "windows-x8664", "macos-aarch64")
```

macOS on x86_64 is absent, which is this project's primary development machine. On an unsupported platform the plugin
prints `Fray JVMTI agent will not be added as a dependency` and the build proceeds — so the racy counter **passed**.
Published artifacts confirm the list: `fray-jvmti-linux-x8664` resolves; every macOS/x86 variant 404s.

Fray is **not** JDK-blocked the way Lincheck is: its own CI matrix runs JDK 11, 21, and 25 across
`ubuntu-latest`, `windows-latest`, and `macos-latest` (Apple Silicon). CI here is `ubuntu-latest` = `linux-x8664`,
which is supported on all three counts — the platform list, the published artifact, and Fray's tested JDK range.

The macOS x86_64 gap is real but not disqualifying. The suite's job is to gate CI, which runs `ubuntu-latest`;
jcstress was never part of a local dev loop either at 30-47 minutes per run. For local reproduction, Docker on
`linux/amd64` is a supported Fray platform on the same hardware.

The gap does create one hazard that must be engineered around: on an unsupported platform Fray skips its agent and
tests **pass**. See "Silent no-op is the failure mode to guard" below.

### Tools considered and not probed

- **Java PathFinder** — effectively pinned to old JDKs; impractical against a Java 25 target.
- **Error Prone `@GuardedBy`** — static lock-discipline checking. Complementary, not a replacement; worth adopting on
  its own merits since `KeyOrderedDispatcher` documents which monitor guards which field in prose only.
- **TLA+** — specification-level. A different layer of the verification story, not a jcstress substitute.

## Consequences

- The 21-class jcstress suite is ported to Fray and jcstress is removed, including its Gradle source sets and the
  dedicated CI job.
- **Memory-model coverage is dropped — and it was never actually present.** Fray controls thread scheduling; it does
  not model hardware reordering, so visibility bugs (unsafe publication, a missing `volatile`, a torn read) are outside
  what it can find. That gap is real. What makes it acceptable is that jcstress was not filling it either:
  `CasPublicationJCStressTest`'s only FORBIDDEN outcome is `1, 0` — reader observes the published handle but a stale
  payload — and x86 does not reorder store-store, so that signature cannot be produced on the only platform CI has ever
  run. Its interesting outcome, `0, 7`, is marked ACCEPTABLE and manifests only off x86. A gate that cannot fail on the
  platform it runs on is the same silent no-op this ADR exists to guard against; retiring it removes a false signal
  rather than a real one.
- CI keeps a dedicated concurrency job, one Gradle task per module holding Fray tests. Measured on `ubuntu-latest`:
  **16 ported classes across 4 modules, 18 explorations, 7,701 schedules, 5m45s** — against the 30-35 minute jcstress
  baseline for 21 classes. Roughly a 6x reduction with schedule coverage counted rather than hoped for.
- Local runs on macOS x86_64 will not exercise Fray. Reproduction uses a `linux/amd64` container.

## Migration results

Measured on `ubuntu-latest`, the CI platform.

| | jcstress | Fray |
| --- | --- | --- |
| Wall clock | ~30-35 min | **5m45s** |
| Classes | 21 | 16 ported |
| Schedules | unbounded stress, outcome histogram | 7,701 explored across 18 runs |
| Planted bug found | still running at 10+ min | **iteration 1, step 852, 764ms** |
| Failure output | forbidden outcome tuple | names the invariant that broke |

The planted-bug row is the one that matters most. The suite was falsified on purpose before being trusted: splitting
`OffsetLedger.markProcessed`'s atomic `computeIfPresent` into a separate empty-check and map removal reopens the
remove-if-empty window, and Fray failed on the first iteration naming the lost offset. A green concurrency suite is
worth nothing until it has been shown to go red, and the probe plus its revert stay in the pilot's history as the
evidence.

**Not ported, with reasons:**

- **Four dispatcher scenarios** (`KeyOrderedEvictRace`, `KeyOrderedEvictTombstone`, `KeyOrderedWorkerHandoff`,
  `DispatcherDrainableCount`) — blocked on the spin-wait constraint below. `@Disabled` carrying that reason.
- **`CasPublication`** — memory-model only; see the consequence above for why it has no successor and why that costs
  less than it appears to.

## Two further constraints the port established

Both were found by a scenario failing to terminate rather than failing an assertion, which is how tooling constraints
tend to present.

- **No live threads when the test body returns.** Fray does not finish an iteration while any thread it started is
  still alive. `KafkaOffsetManager.start()` schedules a periodic commit task that outlives the body, and a scenario
  that started the manager wedged on its first schedule and consumed a whole 30-minute job reporting `Iterations: 0`.
  Components owning a scheduler — the offset manager, the batch wrapper, the circuit breaker, the consumer itself —
  are constructed but not started, and every thread a scenario spawns is joined.
- **Every module carrying Fray tests needs its own instrumentation guard.** The plugin rewires each `Test` task
  independently, so one module instrumenting correctly proves nothing about another; a module whose task was missed
  runs un-instrumented, reports every `@FrayTest` as skipped, and reads green. The guard is a plain `@Test`, because a
  `@FrayTest` would itself be skipped in precisely the state it exists to detect.

## Fray is hostile to sleep-based spin-wait loops

Found in the pilot (PR #310), and the sharpest constraint on the port. A retry loop that waits by calling
`Thread.sleep` cannot be explored to completion by Fray, for reasons that no configuration reaches:

- Fray's launcher sets `sleepAsYield = true`, so a sleeping thread is modelled as *yielding*, never blocked. A
  spin-wait loop is therefore permanently runnable.
- The POS scheduler carries no fairness obligation, so it may re-pick that runnable spinner indefinitely instead of
  advancing the worker the spinner is waiting on.
- `maxScheduledStep` would bound the runaway, but it lives on `ExecutionInfo` as a `val`, and `launchFrayTest` hardcodes
  `-1`. The `additionalConfigs: (Configuration) -> Unit` hook cannot reach it, because `Configuration.executionInfo` is
  itself a `val`.

Observed on `ubuntu-latest`: one iteration reached `step: 32976576` before Fray reported a `DeadlockException`, then
`OutOfMemoryError` in `JsonToStringWriter` while serializing that 33-million-step trace into the run report. The job hit
its 30-minute cap with no verdict. The launcher's other defaults compound it — `launchFrayTest` runs 10,000 iterations
at a 120-second per-iteration timeout, which cannot fit any reasonable CI budget.

**This is a property of the tool, not a defect in the code under test.** `KeyOrderedDispatcher.reserveCapacity` holds no
monitor while it sleeps: it is called before `synchronized (queue)`, and `evictOneIdle` takes the per-queue monitor only
transiently inside `computeIfPresent`. There is no circular wait, and the stalling thread blocks nothing the draining
worker needs. Under any fair scheduler it makes progress, which is what a real JVM provides.

Consequence for the port: `kpipe-consumer` main source has five such loops (`KeyOrderedDispatcher` ×2, `RecordProcessor`
retry backoff, `KPipeConsumer` ×2), and `kpipe-test` has two more. Any ported scenario that drives a thread into one of
them will not terminate under exploration. Scenarios must be built so no thread waits on another by sleeping — or the
invariant must be verified some other way.

## Silent no-op is the failure mode to guard

Both candidates evaluated here **passed** a deliberately racy test when they could not instrument it. That is the
worst possible failure mode for a correctness gate: it converts an untested invariant into a green check.

The suite must therefore carry a permanent self-check that fails the build when Fray is not actually instrumenting,
rather than trusting the plugin's log line. A version bump, a JDK bump, or a new runner architecture can all silently
disable instrumentation, and none of them would otherwise turn CI red.

## Rejected: Lincheck

Kept on record so this is not re-evaluated from documentation alone. Lincheck is well maintained (2.39, 2025-04-09)
and produces the best failure output of the three, but it cannot read Java 25 class files, and it fails open.

## Revisit when

1. **Fray publishes a `macos-x8664` agent, or this machine moves to Apple Silicon.** Local runs become first-class and
   the container wrapper can go.
2. **Memory-model coverage is wanted for real.** The response is not to restore the jcstress suite, which could not
   produce the outcome it was guarding on x86. It is to run `CasPublication` alone on a weakly-ordered runner, where
   its forbidden signature can actually appear — GitHub offers arm64 hosted runners to public repositories, so this is
   one small job rather than a campaign. Until that runs, the honest statement is that kpipe's publication discipline
   is unverified against weak memory ordering, not that it is verified.
3. **Fray stops being maintained.** Re-run the probe against whatever the field offers then.

## Note on method

Both candidates were rejected-or-accepted on evidence produced in about twenty minutes of probing, and both would have
looked fine from their documentation. Any future concurrency-tool evaluation starts the same way: give the tool code
with a known race and confirm it **fails**. A tool that cannot fail is worse than no tool.
