# Plan — Replace DSL-JSON with fastjson2 in `kpipe-format-json`

**Status:** Draft **Created:** 2026-05-24 **Owner:** Mariano **Drivers (in order of weight):**

1. **DSL-JSON is no longer maintained.** Last release on the `com.dslplatform:dsl-json` coordinate is 2.0.2 (mid-2023).
   Two-plus years without a release means no security patches, no JDK-25 fixes, and no recourse if a bug surfaces in
   production. Migrating off is the responsible move regardless of any performance result.
2. **Performance.** fastjson2 claims to beat DSL-JSON on `Map<String,Object>` decode + encode. Validated separately via
   `JsonPipelineBenchmark` (the user will run this; results recorded in this plan under "Bench results"). With
   abandonment as the primary driver, performance becomes a tie-breaker, not a gate — see updated success criteria
   below.

## Scope

The work is contained to `lib/kpipe-format-json/` plus build files.

In-scope files:

- `lib/kpipe-format-json/src/main/java/org/kpipe/format/json/JsonFormat.java`
- `lib/kpipe-format-json/src/main/java/org/kpipe/format/json/JsonConsoleSink.java`
- `lib/kpipe-format-json/src/main/java/module-info.java`
- `lib/kpipe-format-json/src/test/java/org/kpipe/format/json/JsonFormatPipelineTest.java`
- `lib/kpipe-format-json/build.gradle.kts`
- `gradle/libs.versions.toml`

Out-of-scope (public API contract preserved):

- `JsonFormat.INSTANCE`, `JsonFormat.serialize(Map)`, `JsonFormat.deserialize(byte[])`, `JsonFormat.JSON_LOGGING`,
  `JsonFormat.newRegistry()`, `JsonFormat.consoleSink()`
- `MessageFormat<Map<String, Object>>` interface
- `JsonConsoleSink<T>` constructor and `accept(T)` semantics
- Every downstream user (`kpipe-api`, examples, benchmarks) keeps working unchanged.

## Decisions made (from intake)

| Decision            | Choice                                                 |
| ------------------- | ------------------------------------------------------ |
| Motivation          | Performance — validated via `JsonPipelineBenchmark`    |
| Security disclosure | None. Trust fastjson2 defaults.                        |
| Artifact            | `com.alibaba.fastjson2:fastjson2` core only            |
| Reflection policy   | Reflection-free. Explicit `JSONFactory` configuration. |

## Success criteria

Hard gates (all required before merging):

1. `./gradlew :lib:kpipe-format-json:test` passes with the existing test suite — no behavior changes from a caller's
   perspective.
2. `./gradlew check` passes across all modules.
3. `spotlessCheck` passes.
4. `kpipe-format-json` JAR size does not grow by more than 1.5 MB (fastjson2 core is ~1.4 MB vs DSL-JSON's ~150 KB — the
   size delta is ~1.25 MB on the runtime classpath). Acceptable given the maintenance driver; would not be acceptable on
   perf alone.
5. **Bench result is performance parity or better.** Because the migration is justified by abandonment, fastjson2
   doesn't need to beat DSL-JSON to ship — it just can't be materially slower. Concretely: fastjson2 must be no more
   than 15% slower on `decodeOnly`, `encodeOnly`, or `roundTrip`. If fastjson2 is faster, that's a bonus; record the
   delta. If it's between −15% and parity, ship anyway (maintenance > minor perf regression). If it's worse than −15%,
   reconsider — the maintenance argument doesn't justify a 15%+ throughput cliff.

Soft gates (nice-to-have, not blocking):

- `JsonConsoleSink` output identical for the test fixture maps.
- `MessageProcessorRegistryJsonTest` and `JsonConsoleSinkTest` pass without modification.
- fastjson2 wins on at least one cell. Sells the PR description better in the changelog.

## Risks and behavioral deltas to watch

| Risk                                                                                  | Mitigation                                                                                                                                                                                                                                                                                              |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Map type returned by deserialize differs (`LinkedHashMap` vs `JSONObject`)            | fastjson2's `JSONObject` extends `LinkedHashMap`, so `Map<String, Object>` callers are unaffected. Tests asserting `instanceof LinkedHashMap` keep working. Tests asserting the concrete class would break — none currently do (verified by grep).                                                      |
| Number deserialization defaults differ (BigDecimal vs Long vs Integer)                | DSL-JSON returns `Number` subtypes based on JSON form. fastjson2 defaults to `Integer` for small ints, `Long` for larger, `BigDecimal` for fractional. **Test assertions using `.equals()` on map values may break if assertions expect specific subtypes.** Phase 3 task: grep for numeric assertions. |
| `JSON.parseObject(bytes)` is permissive about trailing whitespace; DSL-JSON is strict | Behavioral broadening. Acceptable for a consumer library. No mitigation needed.                                                                                                                                                                                                                         |
| fastjson2 not modular (`Automatic-Module-Name`)                                       | Verify `module-info.java` accepts `requires com.alibaba.fastjson2;`. If it ships only as automatic module, may need `requires <auto-name>;` syntax. Confirmed in fastjson2 ≥ 2.0.45 the JAR ships `Automatic-Module-Name: com.alibaba.fastjson2`.                                                       |
| fastjson2 brand association with v1 RCE history                                       | Trusting v2 defaults per intake decision. Autotype disabled by default in v2; explicitly pin off via `JSONFactory.disableAutoType()` in static initializer to be safe (zero cost, removes the obvious config-mistake risk).                                                                             |
| `kpipe-format-json` JAR-size growth (~150KB → ~1.4MB)                                 | Acceptable trade for performance. Quantify in phase 5 commit message.                                                                                                                                                                                                                                   |
| fastjson2 has its own internal caches that may leak across class-loader boundaries    | Not relevant for KPipe's single-classloader use. Document as a known limitation if it ever becomes a multi-tenant concern.                                                                                                                                                                              |

## Tracer-bullet phases

Each phase is independently mergeable and leaves the project in a working state. Phases 1–4 can land as a single commit
if Phase 5 (the bench) passes; if it fails, phases 1–4 get reverted as a unit.

### Phase 0 — Capture DSL-JSON baseline (~30 min)

**Goal:** Establish the bar fastjson2 must clear.

Steps:

1. Run `JsonPipelineBenchmark` against current `main` with
   `-Pjmh.fork=2 -Pjmh.iterations=5 -Pjmh.warmupIterations=3 -Pjmh.profilers=gc -Pjmh.resultFormat=JSON`.
2. Save output to `benchmarks/results/2026-05-24-baseline-dsljson.json`.
3. Record the headline numbers (decodeOnly, encodeOnly, roundTrip throughputs and B/op) in this plan under "Bench
   results" section below.

**Done when:** Baseline JSON and numbers are captured.

### Phase 1 — Add fastjson2 dependency alongside DSL-JSON (~15 min)

**Goal:** Bring fastjson2 onto the classpath without changing any code. Verify JPMS plays nice.

Steps:

1. `gradle/libs.versions.toml`: add `fastjson2 = "2.0.55"` (or latest stable at time of work),
   `fastjson2 = { module = "com.alibaba.fastjson2:fastjson2", version.ref = "fastjson2" }`.
2. `lib/kpipe-format-json/build.gradle.kts`: add `implementation(libs.fastjson2)` (keep `implementation(libs.dslJson)`
   for now).
3. `lib/kpipe-format-json/src/main/java/module-info.java`: add `requires com.alibaba.fastjson2;` alongside the existing
   `requires dsl.json;`.
4. `./gradlew :lib:kpipe-format-json:compileJava` to confirm the module compiles with both deps present.

**Done when:** Build succeeds with both libraries on the classpath.

### Phase 2 — Port `JsonFormat` to fastjson2 (~45 min)

**Goal:** Replace `DSL_JSON.serialize` / `DSL_JSON.deserialize` calls with fastjson2 equivalents. Configure
reflection-free mode.

Steps:

1. Edit `JsonFormat.java`:
   - Replace `import com.dslplatform.json.DslJson;` with `import com.alibaba.fastjson2.JSON;` and
     `import com.alibaba.fastjson2.JSONFactory;`.
   - Replace `private static final DslJson<Map<String, Object>> DSL_JSON = new DslJson<>();` with a static initializer
     that calls `JSONFactory.disableAutoType()` once. No instance field needed — fastjson2's `JSON.toJSONBytes(map)` and
     `JSON.parseObject(bytes)` are stateless static methods.
   - `serialize(Map)`: replace the try-with-resources `ByteArrayOutputStream` block with `JSON.toJSONBytes(data)`. Wraps
     `JSONException` → `RuntimeException` to preserve the existing exception contract.
   - `deserialize(byte[])`: replace the try-with-resources `ByteArrayInputStream` block with `JSON.parseObject(data)`
     (returns `JSONObject` which is a `LinkedHashMap`). Cast to `Map<String, Object>`.
2. Run `./gradlew :lib:kpipe-format-json:test` and let it fail loudly on any behavioral delta.
3. Triage failures one by one — most likely culprits are numeric subtype assertions (`Long` vs `Integer`) and null
   handling on empty objects.

**Done when:** `JsonFormat`'s tests pass with fastjson2 powering serialize / deserialize.

### Phase 3 — Port `JsonConsoleSink` and test fixtures (~30 min)

**Goal:** Drop DSL-JSON from `JsonConsoleSink` and from the test that uses it directly.

Steps:

1. `JsonConsoleSink.java`: same pattern as `JsonFormat`. Replace the `DslJson<Object>` instance field with a static
   `JSON.toJSONString(value)` call. Pretty-printing in fastjson2:
   `JSON.toJSONString(value, JSONWriter.Feature.PrettyFormat)`.
2. `JsonFormatPipelineTest.java`: line 16 uses `DslJson` directly for round-trip verification. Replace with
   `JSON.parseObject` / `JSON.toJSONBytes`.
3. Run `:lib:kpipe-format-json:test` — should be green now.

**Done when:** No source file in `kpipe-format-json/src/` imports `com.dslplatform.json.DslJson`.

### Phase 4 — Cross-module verification (~15 min)

**Goal:** Confirm nothing downstream broke.

Steps:

1. `./gradlew :lib:kpipe-api:test :lib:kpipe-consumer:test :lib:kpipe-producer:test :benchmarks:compileJmhJava`. All
   green.
2. `./gradlew :examples:json:compileJava :examples:demo:compileJava` to verify the example apps still compile.
3. `./gradlew spotlessApply spotlessCheck`. Clean.

**Done when:** Full check passes (excluding Docker-dependent integration tests, which are unrelated to the JSON library
swap).

### Phase 5 — Benchmark, sanity check (~30 min, user-executed)

**Goal:** Verify fastjson2 isn't a performance regression worse than the maintenance win is worth. NOT a go/no-go gate
in the strict sense — the maintenance driver carries the migration even if fastjson2 is slightly slower.

Steps:

1. Run `JsonPipelineBenchmark` against the fastjson2 branch with the same JMH config as Phase 0
   (`-Pjmh.fork=2 -Pjmh.iterations=5 -Pjmh.warmupIterations=3 -Pjmh.profilers=gc -Pjmh.resultFormat=JSON`).
2. Save output to `benchmarks/results/2026-05-24-fastjson2.json`.
3. Fill in the "Bench results" comparison table below.
4. **Sanity check (not a hard gate):**
   - fastjson2 ≥ DSL-JSON on all cells → ship, lead the PR description with the perf win.
   - fastjson2 between −15% and 0% on any cell → ship anyway. Lead the PR with the maintenance argument; mention perf as
     a wash.
   - fastjson2 worse than −15% on any cell → pause. Investigate (likely reflection-free config not applied, or a default
     that needs tuning). If unresolvable, document the surprise in this plan but ship anyway — running on an
     unmaintained library is the bigger risk. The library swap is the goal; the perf was a hope.

**Done when:** Bench numbers recorded and shipping decision documented in "Outcome" below.

### Phase 6 — Remove DSL-JSON (~15 min)

**Goal:** Only if Phase 5 gate passes. Drop DSL-JSON entirely.

Steps:

1. `lib/kpipe-format-json/build.gradle.kts`: remove `implementation(libs.dslJson)`, `annotationProcessor(libs.dslJson)`,
   `testAnnotationProcessor(libs.dslJson)`.
2. `gradle/libs.versions.toml`: remove the `dslJson = ...` version entry and the library coordinates entry.
3. `module-info.java`: remove `requires dsl.json;`.
4. `./gradlew clean :lib:kpipe-format-json:test` to confirm a fresh build still works without DSL-JSON anywhere on the
   classpath.

**Done when:** DSL-JSON is gone from `libs.versions.toml`, no source file imports it, and the build is green from a
clean state.

### Phase 7 — Documentation cleanup (~15 min)

**Goal:** Update any documentation that mentions DSL-JSON.

Steps:

1. Grep for `dsl-json`, `DSL-JSON`, `DslJson`, `dslplatform` across:
   - `README.md`
   - `.claude/CLAUDE.md`
   - `docs/escape-hatches.md`
   - `benchmarks/METHODOLOGY.md`
   - Module-info Javadoc
2. Replace mentions where appropriate. The `MessageFormat.java` Javadoc (in `kpipe-core`) mentions "JSON via DSL-JSON" —
   update to "JSON via fastjson2".
3. Commit message for the squashed PR should explicitly mention the dependency swap and include the bench delta numbers
   from Phase 5.

**Done when:** No project documentation references DSL-JSON.

## Bench results

To be filled in during Phase 0 (baseline) and Phase 5 (fastjson2).

| Cell         | DSL-JSON (baseline) | fastjson2 | Δ throughput | DSL-JSON B/op | fastjson2 B/op | Δ alloc |
| ------------ | ------------------: | --------: | -----------: | ------------: | -------------: | ------: |
| `decodeOnly` |                 TBD |       TBD |          TBD |           TBD |            TBD |     TBD |
| `encodeOnly` |                 TBD |       TBD |          TBD |           TBD |            TBD |     TBD |
| `roundTrip`  |                 TBD |       TBD |          TBD |           TBD |            TBD |     TBD |

## Outcome

To be filled in after Phase 5.

Bench result category: [ ] Win / [ ] Wash / [ ] Regression within tolerance / [ ] Regression beyond tolerance

Shipping decision: [ ] Shipped — perf win [ ] Shipped — maintenance-driven [ ] Paused — investigating

Notes:

## Effort estimate

| Phase                              | Time      |
| ---------------------------------- | --------- |
| 0 — Baseline bench                 | 30 min    |
| 1 — Add fastjson2 dependency       | 15 min    |
| 2 — Port `JsonFormat`              | 45 min    |
| 3 — Port `JsonConsoleSink` + tests | 30 min    |
| 4 — Cross-module verification      | 15 min    |
| 5 — Bench + gate decision          | 30 min    |
| 6 — Remove DSL-JSON                | 15 min    |
| 7 — Documentation cleanup          | 15 min    |
| **Total (if gate passes)**         | **~3 hr** |
| Total (if gate fails, abandon)     | ~2 hr     |

## Open questions

1. **Which fastjson2 version to pin?** Default to the latest stable on Maven Central at time of execution. As of Q2
   2026, that's around 2.0.55. Verify before pinning.

2. **Do we want a runtime check that fastjson2 is on the classpath, with a useful error if not?** No — Maven dependency
   resolution handles this. Compile error if missing is fine.

3. **Should the migration include adding `JsonPipelineBenchmark.fastjson2()` as a permanent competitive cell?** Probably
   yes if the migration ships — gives future contributors a way to verify the bench delta hasn't regressed. Worth a
   follow-up issue, not in this PR.
