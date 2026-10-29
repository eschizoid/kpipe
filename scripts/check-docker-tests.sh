#!/usr/bin/env bash
# Fails when a Testcontainers test class did not actually run.
#
# A class annotated `@Testcontainers(disabledWithoutDocker = true)` is skipped, not failed, when no
# Docker daemon is reachable, and the build stays green. Run this after the test task: every such
# class must have a JUnit XML report with at least one test that was not skipped.
set -euo pipefail

cd "$(dirname "$0")/.."

python3 - <<'EOF'
import pathlib, subprocess, sys, xml.etree.ElementTree as ET

files = subprocess.run(
    ["git", "grep", "-l", "disabledWithoutDocker = true", "--", "*/src/test/java/*.java"],
    capture_output=True, text=True, check=True,
).stdout.split()

problems = []
for source in files:
    module, _, rest = source.partition("/src/test/java/")
    test_class = rest.removesuffix(".java").replace("/", ".")
    report = pathlib.Path(module, "build", "test-results", "test", f"TEST-{test_class}.xml")
    if not report.is_file():
        problems.append(f"no test report: {test_class}")
        continue
    suite = ET.parse(report).getroot()
    tests, skipped = int(suite.get("tests", 0)), int(suite.get("skipped", 0))
    if tests == 0 or skipped == tests:
        problems.append(f"skipped ({skipped} of {tests}): {test_class}")

if problems:
    print("Docker-backed test classes that did not run:")
    print("\n".join(f"  {p}" for p in problems))
    sys.exit(1)
print(f"check-docker-tests: all {len(files)} Docker-backed test classes ran")
EOF
