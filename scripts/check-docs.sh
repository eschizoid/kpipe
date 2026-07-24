#!/usr/bin/env bash
# Documentation consistency checks, run in CI (see .github/workflows/ci.yaml).
#
#  1. Every relative markdown link in README.md, docs/*.md, and benchmarks/README.md resolves to
#     a file or directory that exists.
#  2. Every io.github.eschizoid:kpipe-* coordinate in README.md and docs/MODULES.md carries the
#     same version string (no half-bumped install snippets after a release).
#  3. The README's thirty-second example has not drifted from the compiled class it claims to be
#     (every code line of the README block must appear in ReadmeQuickstart.java).
set -uo pipefail
cd "$(dirname "$0")/.."
fail=0

# --- 1. relative links resolve -------------------------------------------------------------
for f in README.md docs/*.md benchmarks/README.md; do
  dir=$(dirname "$f")
  # capture [text](target) links, drop external/anchor-only targets, strip #fragments
  while read -r target; do
    [ -z "$target" ] && continue
    case "$target" in
      http*|mailto:*) continue ;;
    esac
    path="${target%%#*}"
    [ -z "$path" ] && continue
    if [ ! -e "$dir/$path" ]; then
      echo "BROKEN LINK in $f: $target"
      fail=1
    fi
  done < <(grep -oE '\]\(([^)#][^)]*)\)' "$f" | sed -E 's/^\]\(//; s/\)$//' || true)
done

# --- 2. one version string everywhere ------------------------------------------------------
versions=$(grep -rhoE 'io\.github\.eschizoid:kpipe[a-z-]*:[0-9]+\.[0-9]+\.[0-9]+' README.md docs/MODULES.md \
  | sed -E 's/.*:([0-9]+\.[0-9]+\.[0-9]+)$/\1/' | sort -u)
count=$(echo "$versions" | grep -c . || true)
if [ "$count" -gt 1 ]; then
  echo "VERSION DRIFT: multiple kpipe versions referenced in docs: $(echo "$versions" | tr '\n' ' ')"
  fail=1
fi

# --- 3. quickstart snippet matches the compiled class --------------------------------------
quickstart=examples/json/src/main/java/io/github/eschizoid/kpipe/ReadmeQuickstart.java
snippet=$(awk '/^```java$/{grab=1; next} /^```$/{if (grab) exit} grab' README.md)
if [ -z "$snippet" ]; then
  echo "QUICKSTART CHECK: no java block found in README.md"
  fail=1
else
  while IFS= read -r line; do
    trimmed=$(echo "$line" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//')
    [ -z "$trimmed" ] && continue
    if ! grep -qF "$trimmed" "$quickstart"; then
      echo "QUICKSTART DRIFT: README line not in $quickstart: $trimmed"
      fail=1
    fi
  done <<< "$snippet"
fi

if [ "$fail" -ne 0 ]; then
  echo "check-docs: FAILED"
  exit 1
fi
echo "check-docs: OK"
