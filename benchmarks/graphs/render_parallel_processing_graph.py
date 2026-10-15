#!/usr/bin/env python3
"""Render the competitive ParallelProcessingBenchmark graph from a gc-profiled JMH results JSON.

    python3 benchmarks/graphs/render_parallel_processing_graph.py \
      benchmarks/results/2026-10-12-full-suite.json \
      --host "Apple M3 Max" --capture "2026-10-12 (Atleon rerun 2026-10-13)"

Standard library only. JDK, forks and iterations come from the JSON; the host and capture label
do not, because JMH does not record them.
"""
import argparse
import json
import math
import os
from xml.sax.saxutils import escape

ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument("results", help="JMH results JSON from a run that included -prof gc")
ap.add_argument("-o", "--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                    "parallel_processing_gc_comparison.svg"))
ap.add_argument("--host", required=True, help='e.g. "Apple M3 Max"')
ap.add_argument("--capture", required=True, help='capture date label, e.g. "2026-10-12"')
ap.add_argument("--kafka", default="4.3.0", help="broker version used by Testcontainers")
args = ap.parse_args()
SRC, OUT = args.results, args.out
data = json.load(open(SRC))

ARMS = {  # arm -> (label, css class, color)
    "raw": ("Raw KafkaConsumer + VT", "raw", "#14b8a6"),
    "kpipe": ("KPipe PARALLEL", "kpipe", "#22c55e"),
    "atleonParallel": ("Atleon parallel", "atleon", "#f59e0b"),
    "confluent": ("Confluent PC (UNORDERED)", "confluent", "#3b82f6"),
    "reactor": ("Reactor Kafka", "reactor", "#8b5cf6"),
    "share": ("Kafka Share Consumer", "share", "#64748b"),
    "kpipeKeyOrdered": ("KPipe KEY_ORDERED", "kpipe", "#22c55e"),
    "atleonKey": ("Atleon key-ordered", "atleon", "#f59e0b"),
    "confluentKey": ("Confluent PC (KEY)", "confluent", "#3b82f6"),
}
UNORDERED = ["raw", "kpipe", "atleonParallel", "confluent", "reactor", "share"]
PER_KEY = ["kpipeKeyOrdered", "atleonKey", "confluentKey"]
WORK = [0, 100, 1000]
WORK_NOTE = {0: "pure framework overhead", 100: "local enrichment", 1000: "blocking I/O round trip"}

res = {}
for e in data:
    if "ParallelProcessingBenchmark." not in e["benchmark"] or e["mode"] != "thrpt":
        continue
    arm = e["benchmark"].rsplit(".", 1)[1]
    w = int(e["params"]["workMicros"])
    pm, sm = e["primaryMetric"], e["secondaryMetrics"]
    res[(arm, w)] = {
        "score": pm["score"],
        "err": pm["scoreError"] if isinstance(pm["scoreError"], (int, float)) else 0.0,
        "alloc": sm.get("·gc.alloc.rate.norm", {}).get("score"),
        "count": sm.get("·gc.count", {}).get("score"),
        "time": sm.get("·gc.time", {}).get("score"),
    }
missing = [(a, w) for a in UNORDERED + PER_KEY for w in WORK if (a, w) not in res]
assert not missing, f"missing results: {missing}"


def fmt(v):
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if v >= 10_000:
        return f"{v / 1000:.0f}k"
    if v >= 1000:
        return f"{v / 1000:.1f}k"
    return f"{v:.0f}"


W = 1180
out = []
S = out.append


def text(x, y, s, cls, anchor="start", extra=""):
    S(f'<text x="{x:.1f}" y="{y:.1f}" class="{cls}" text-anchor="{anchor}"{extra}>{escape(s)}</text>')


def grouped_panel(y0, h, title, arms):
    S(f'<rect x="30" y="{y0}" width="{W - 60}" height="{h}" rx="10" class="panel"/>')
    text(50, y0 + 28, title, "h2")
    text(50, y0 + 46, "records / second · higher is better · whiskers are JMH 99.9% error", "small")
    raw_top = max(res[(a, w)]["score"] + res[(a, w)]["err"] for a in arms for w in WORK)
    step = 10 ** math.floor(math.log10(raw_top / 4))
    step = next(m * step for m in (1, 2, 2.5, 5, 10) if m * step * 4 >= raw_top)
    top = step * 4
    px0, px1 = 90, W - 50
    py0, py1 = y0 + 70, y0 + h - 52
    ph = py1 - py0
    for i in range(5):
        v = top * i / 4
        y = py1 - ph * i / 4
        S(f'<line x1="{px0}" y1="{y:.1f}" x2="{px1}" y2="{y:.1f}" class="grid"/>')
        text(px0 - 8, y + 4, fmt(v), "small", "end")
    S(f'<line x1="{px0}" y1="{py1}" x2="{px1}" y2="{py1}" class="axis"/>')
    gw = (px1 - px0) / len(WORK)
    bw = min(46, (gw - 50) / len(arms))
    for gi, w in enumerate(WORK):
        gx = px0 + gi * gw + (gw - bw * len(arms)) / 2
        for ai, a in enumerate(arms):
            r = res[(a, w)]
            x = gx + ai * bw
            bh = ph * r["score"] / top
            S(f'<rect x="{x + 3:.1f}" y="{py1 - bh:.1f}" width="{bw - 6:.1f}" height="{bh:.1f}" fill="{ARMS[a][2]}" rx="3"/>')
            if r["err"]:
                cx = x + bw / 2
                lo = py1 - ph * max(r["score"] - r["err"], 0) / top
                hi = py1 - ph * (r["score"] + r["err"]) / top
                S(f'<line x1="{cx:.1f}" y1="{lo:.1f}" x2="{cx:.1f}" y2="{hi:.1f}" class="whisker"/>')
                S(f'<line x1="{cx - 5:.1f}" y1="{hi:.1f}" x2="{cx + 5:.1f}" y2="{hi:.1f}" class="whisker"/>')
            text(x + bw / 2, min(py1 - bh, hi if r["err"] else py1 - bh) - 6, fmt(r["score"]), "value", "middle")
        text(px0 + gi * gw + gw / 2, py1 + 20, f"workMicros = {w}", "label", "middle")
        text(px0 + gi * gw + gw / 2, py1 + 36, WORK_NOTE[w], "small", "middle")


def legend(y, arms):
    x = 50
    for a in arms:
        lab = ARMS[a][0]
        S(f'<rect x="{x}" y="{y - 10}" width="14" height="14" rx="3" fill="{ARMS[a][2]}"/>')
        text(x + 20, y + 2, lab, "label")
        x += 32 + 7.2 * len(lab)


def hbar_panel(x0, y0, w, h, title, sub, key, arms):
    S(f'<rect x="{x0}" y="{y0}" width="{w}" height="{h}" rx="10" class="panel"/>')
    text(x0 + 16, y0 + 24, title, "h3")
    text(x0 + 16, y0 + 40, sub, "small")
    rows = sorted(arms, key=lambda a: res[(a, 0)][key] or 0)
    top = max(res[(a, 0)][key] or 0 for a in rows) or 1
    bx, bmax = x0 + 136, w - 136 - 60
    rh = (h - 60) / len(rows)
    for i, a in enumerate(rows):
        v = res[(a, 0)][key] or 0
        y = y0 + 54 + i * rh
        text(bx - 8, y + rh / 2 + 4, ARMS[a][0].replace("Raw KafkaConsumer + VT", "Raw + VT")
             .replace("Confluent PC ", "Confl. ").replace("Kafka Share Consumer", "Share"), "small", "end")
        bl = bmax * v / top
        S(f'<rect x="{bx}" y="{y + 3:.1f}" width="{max(bl, 1):.1f}" height="{rh - 6:.1f}" rx="2" fill="{ARMS[a][2]}"/>')
        text(bx + bl + 6, y + rh / 2 + 4, f"{v:,.0f}", "value")


H = 1250
S(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" role="img" '
  'aria-label="KPipe vs Atleon vs Confluent Parallel Consumer vs Reactor Kafka vs Kafka Share Consumer vs raw '
  'KafkaConsumer plus virtual threads: parallel-consumer throughput by ordering guarantee, allocation, and GC">')
S("""<defs><style>
.bg { fill: #ffffff; } .panel { fill: #f8fafc; stroke: #cbd5e1; stroke-width: 1; }
.title { fill: #111827; font: 700 24px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; }
.subtitle { fill: #4b5563; font: 500 13px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; }
.h2 { fill: #111827; font: 700 16px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; }
.h3 { fill: #1f2937; font: 700 14px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; }
.axis { stroke: #9ca3af; stroke-width: 1; } .grid { stroke: #e5e7eb; stroke-width: 1; stroke-dasharray: 4 4; }
.whisker { stroke: #111827; stroke-width: 1.4; }
.label { fill: #374151; font: 600 12px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; }
.small { fill: #4b5563; font: 11px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; }
.value { fill: #111827; font: 700 11px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; }
.note { fill: #374151; font: 12.5px -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; }
</style></defs>""")
S(f'<rect width="{W}" height="{H}" class="bg"/>')
text(30, 40, "ParallelProcessingBenchmark: competitive throughput and GC", "title")
first = next(e for e in data if "ParallelProcessingBenchmark." in e["benchmark"])
text(30, 64, f"JMH throughput + gc profiler · 25,000 records · 8 partitions · 1,000 keys · forks={first['forks']} · "
             f"iter={first['measurementIterations']} · Testcontainers Kafka {args.kafka} · {args.host} · "
             f"JDK {first['jdkVersion']} · capture {args.capture}", "subtitle")

legend(100, UNORDERED)
grouped_panel(116, 330, "Unordered arms", UNORDERED)
legend(482, PER_KEY)
grouped_panel(498, 300, "Per-key ordered arms (compare these with each other, not with the unordered panel)", PER_KEY)

text(30, 834, "Cost profile at workMicros = 0", "h2")
gc_arms = UNORDERED + PER_KEY
pw = (W - 60 - 2 * 20) / 3
hbar_panel(30, 848, pw, 230, "gc.alloc.rate.norm (B/op)", "bytes allocated per record · lower is better",
           "alloc", gc_arms)
hbar_panel(30 + pw + 20, 848, pw, 230, "gc.count", "GC events in the measurement window", "count", gc_arms)
hbar_panel(30 + 2 * (pw + 20), 848, pw, 230, "gc.time (ms)", "GC time in the measurement window", "time", gc_arms)

r = lambda a, w: res[(a, w)]["score"]


def per_work(a, b):
    """The ratio a/b at each workMicros, in WORK order, e.g. '1.20x / 0.95x / 1.10x'."""
    return " / ".join(f"{r(a, w) / r(b, w):.2f}x" for w in WORK)


def scores(a):
    return " / ".join(fmt(r(a, w)) for w in WORK)


work_label = " / ".join(str(w) for w in WORK)
notes = [
    f"• Unordered, records/s at workMicros {work_label}: KPipe {scores('kpipe')}, Atleon {scores('atleonParallel')}, "
    f"raw {scores('raw')}.",
    f"• KPipe ÷ Atleon: {per_work('kpipe', 'atleonParallel')} unordered, {per_work('kpipeKeyOrdered', 'atleonKey')} "
    f"per key. KPipe ÷ raw: {per_work('kpipe', 'raw')}.",
    f"• KPipe KEY_ORDERED ÷ Confluent PC KEY: {per_work('kpipeKeyOrdered', 'confluentKey')}.",
    "• gc.count and gc.time are totals for the measurement window, so faster arms run more invocations and collect more.",
    "  Per-partition arms and the single-threaded baseline are omitted: different guarantee. "
    f"Full table: benchmarks/results/{os.path.splitext(os.path.basename(SRC))[0]}.md",
]
for i, n in enumerate(notes):
    text(30, 1110 + i * 20, n, "note")
S("</svg>")
open(OUT, "w").write("\n".join(out) + "\n")
print(f"wrote {OUT}: {sum(len(l) for l in out):,} bytes")
