#!/usr/bin/env python3
"""results/*.csv -> median tables (results/summary.md) + charts (results/*.png)."""
import csv, glob, os, statistics as st
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(ROOT, "results")
BLUE, ORANGE = "#2a78d6", "#eb6834"          # validated categorical slots 1 and 2 (REST, gRPC)
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
COL = {"rest": BLUE, "grpc": ORANGE}
plt.rcParams.update({"font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED,
                     "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
                     "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb"})

# SIS #1 payload shapes used in the `sizes` suite: all at target 3000 rps.
SIZE_SHAPES = [(100, 3000), (1024, 3000), (10240, 3000)]


def load(name):
    p = os.path.join(RES, name)
    if not os.path.exists(p):
        return []
    return list(csv.DictReader(open(p)))


def medians(rows):
    """(proto,mode,size,rps) -> dict of median metrics over runs"""
    g = defaultdict(list)
    for r in rows:
        g[(r["proto"], r["mode"], int(r["size"]), int(r["rps_target"]))].append(r)
    out = {}
    for k, rs in g.items():
        m = {c: st.median(float(r[c]) for r in rs) for c in
             ["rps_achieved", "errors", "p50_us", "p90_us", "p95_us", "p99_us", "p999_us", "max_us", "svc_p50_us", "svc_p99_us"]}
        m["runs"] = len(rs)
        m["p99_spread"] = (min(float(r["p99_us"]) for r in rs), max(float(r["p99_us"]) for r in rs))
        out[k] = m
    return out


def ms(us):
    return us / 1000.0


def fmt(us):
    return f"{us/1000:.2f}" if us < 100_000 else f"{us/1000:.0f}"


def human(n):
    return f"{n//1024} KB" if n >= 1024 and n % 1024 == 0 else (f"{n} B" if n < 1000 else f"{n/1000:g} KB")


def table(med, keys):
    lines = ["| Protocol | Payload | RPS | achieved | p50 ms | p90 | p95 | p99 | p99.9 | max | p99 min-max over runs | errors |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for k in keys:
        if k not in med:
            continue
        m = med[k]
        proto, mode, size, rps = k
        pl = human(size)
        lines.append(f"| {proto} | {pl} | {rps} | {m['rps_achieved']:.0f} | {fmt(m['p50_us'])} | {fmt(m['p90_us'])} | {fmt(m['p95_us'])} | "
                     f"{fmt(m['p99_us'])} | {fmt(m['p999_us'])} | {fmt(m['max_us'])} | {fmt(m['p99_spread'][0])}-{fmt(m['p99_spread'][1])} | {int(m['errors'])} |")
    return "\n".join(lines)


def grouped_bars(ax, cats, series, ylabel, title, log=True):
    w = 0.38
    for i, (name, vals) in enumerate(series.items()):
        xs = [c + (i - 0.5) * (w + 0.04) for c in range(len(cats))]
        bars = ax.bar(xs, vals, w, color=COL[name], label=name.upper() if name == "rest" else "gRPC",
                      edgecolor="#fcfcfb", linewidth=1.5)
        for x, v in zip(xs, vals):
            ax.text(x, v * (1.08 if log else 1.0) + (0 if log else 0), f"{v:.1f}" if v < 100 else f"{v:.0f}",
                    ha="center", va="bottom", fontsize=8, color=INK)
    ax.set_xticks(range(len(cats)))
    ax.set_xticklabels(cats)
    if log:
        ax.set_yscale("log")
    ax.set_ylabel(ylabel)
    ax.set_title(title, loc="left", color=INK, fontsize=11)
    ax.grid(axis="x", visible=False)


def chart_sizes(med):
    cats = [f"{human(s)}\n@ {r} rps" for s, r in SIZE_SHAPES]
    fig, axs = plt.subplots(1, 2, figsize=(11, 4.2))
    for ax, metric, ttl in zip(axs, ["p50_us", "p99_us"], ["Median latency (p50)", "Tail latency (p99)"]):
        series = {p: [ms(med[(p, "text", s, r)][metric]) for s, r in SIZE_SHAPES if (p, "text", s, r) in med] for p in ("rest", "grpc")}
        if all(len(v) == len(SIZE_SHAPES) for v in series.values()):
            grouped_bars(ax, cats, series, "ms", ttl, log=False)
            ax.margins(y=0.18)
    h, l = axs[0].get_legend_handles_labels()
    fig.legend(h, l, frameon=False, loc="upper right", ncol=2, bbox_to_anchor=(0.99, 0.99))
    fig.suptitle("Latency vs payload size at target 3000 rps (open-loop, warm-up discarded, median of 3 runs)",
                 x=0.01, ha="left", color=INK, fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(os.path.join(RES, "latency_vs_payload.png"), dpi=150)
    plt.close(fig)


def chart_rps(med):
    rates = sorted({k[3] for k in med if k[1] == "text" and k[2] == 1024})
    if len(rates) < 3:
        return
    fig, axs = plt.subplots(1, 2, figsize=(11, 4.2))
    for ax, metric, ttl in zip(axs, ["p50_us", "p99_us"], ["Median latency (p50)", "Tail latency (p99)"]):
        for p in ("rest", "grpc"):
            ys = [ms(med[(p, "text", 1024, r)][metric]) for r in rates if (p, "text", 1024, r) in med]
            xs = [r for r in rates if (p, "text", 1024, r) in med]
            ax.plot(xs, ys, color=COL[p], linewidth=2, marker="o", markersize=6, markeredgecolor="#fcfcfb", markeredgewidth=2,
                    label="REST/JSON" if p == "rest" else "gRPC")
            ax.annotate("REST/JSON" if p == "rest" else "gRPC", (xs[-1], ys[-1]), textcoords="offset points", xytext=(6, 0),
                        va="center", fontsize=9, color=INK)
        ax.set_xscale("log"); ax.set_yscale("log")
        ax.set_xticks(rates); ax.set_xticklabels([str(r) for r in rates]); ax.minorticks_off()
        ax.set_xlabel("target requests per second"); ax.set_ylabel("ms (log scale)")
        ax.set_title(ttl, loc="left", color=INK, fontsize=11)
        ax.margins(x=0.12)
    axs[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Latency vs load, 1 KB payload (median of 3 runs)", x=0.01, ha="left", color=INK, fontsize=12)
    fig.tight_layout()
    fig.savefig(os.path.join(RES, "latency_vs_rps.png"), dpi=150)
    plt.close(fig)


def chart_ramp(med):
    """Capacity ramp: achieved rps + corrected p99 vs target rps. All points kept, including saturated ones."""
    rates = sorted({k[3] for k in med if k[1] == "text" and k[2] == 1024})
    if len(rates) < 3:
        return
    fig, axs = plt.subplots(1, 2, figsize=(11, 4.2))
    # left: achieved rps vs target
    ax = axs[0]
    for p in ("rest", "grpc"):
        xs = [r for r in rates if (p, "text", 1024, r) in med]
        ys = [med[(p, "text", 1024, r)]["rps_achieved"] for r in xs]
        ax.plot(xs, ys, color=COL[p], linewidth=2, marker="o", markersize=6, markeredgecolor="#fcfcfb",
                markeredgewidth=2, label="REST/JSON" if p == "rest" else "gRPC")
    ax.plot(rates, rates, color=MUTED, linewidth=1, linestyle="--", label="target = achieved")
    ax.set_xlabel("target rps"); ax.set_ylabel("achieved rps")
    ax.set_title("Achieved rps (capacity ceiling)", loc="left", color=INK, fontsize=11)
    ax.legend(frameon=False, loc="upper left")
    # right: p99 (corrected, open-loop)
    ax = axs[1]
    for p in ("rest", "grpc"):
        xs = [r for r in rates if (p, "text", 1024, r) in med]
        ys = [ms(med[(p, "text", 1024, r)]["p99_us"]) for r in xs]
        ax.plot(xs, ys, color=COL[p], linewidth=2, marker="o", markersize=6, markeredgecolor="#fcfcfb",
                markeredgewidth=2, label="REST/JSON" if p == "rest" else "gRPC")
    ax.set_yscale("log")
    ax.set_xlabel("target rps"); ax.set_ylabel("corrected p99 ms (log)")
    ax.set_title("p99 under load (with queueing)", loc="left", color=INK, fontsize=11)
    fig.suptitle("Capacity ramp, 1 KB, --threads 512 (median of 3 runs; all points kept)",
                 x=0.01, ha="left", color=INK, fontsize=12)
    fig.tight_layout()
    fig.savefig(os.path.join(RES, "latency_ramp.png"), dpi=150)
    plt.close(fig)


def chart_wire():
    rows = load("payload_wire.csv")
    if not rows:
        return
    fig, ax = plt.subplots(1, 1, figsize=(7.5, 4.4))
    sizes = sorted({int(r["size"]) for r in rows if r["mode"] == "text"})
    variants = [("rest", False), ("rest", True), ("grpc", False), ("grpc", True)]
    w = 0.2
    for i, (p, gz) in enumerate(variants):
        vals = []
        for s in sizes:
            r = next(r for r in rows if r["mode"] == "text" and int(r["size"]) == s and r["proto"] == (p + ("-gzip" if gz else "")))
            vals.append(int(r["steady_c2s"]) + int(r["steady_s2c"]))
        xs = [j + (i - 1.5) * (w + 0.02) for j in range(len(sizes))]
        ax.bar(xs, vals, w, color=COL[p], hatch="////" if gz else None, edgecolor="#fcfcfb", linewidth=1.5,
               label=("REST/JSON" if p == "rest" else "gRPC") + (" + gzip" if gz else ""))
    ax.set_yscale("log"); ax.set_xticks(range(len(sizes))); ax.set_xticklabels([human(s) for s in sizes])
    ax.set_ylabel("bytes per request+response (log)")
    ax.set_title("One event, text body", loc="left", color=INK, fontsize=11)
    ax.grid(axis="x", visible=False)
    ax.legend(frameon=False, loc="upper left", fontsize=8)
    fig.suptitle("On-wire bytes per request, steady state (headers + framing included, plaintext)",
                 x=0.01, ha="left", color=INK, fontsize=12)
    fig.tight_layout()
    fig.savefig(os.path.join(RES, "payload_on_wire.png"), dpi=150)
    plt.close(fig)


def wire_table():
    rows = load("payload_wire.csv")
    out = ["| Payload | REST | REST+gzip | gRPC | gRPC+gzip | gRPC / REST |", "|---|---|---|---|---|---|"]
    for s in sorted({int(r["size"]) for r in rows if r["mode"] == "text"}):
        vals = {}
        for r in rows:
            if r["mode"] == "text" and int(r["size"]) == s:
                vals[r["proto"]] = int(r["steady_c2s"]) + int(r["steady_s2c"])
        out.append(f"| {human(s)} body | {vals['rest']} | {vals['rest-gzip']} | {vals['grpc']} | {vals['grpc-gzip']} | {vals['grpc']/vals['rest']:.2f} |")
    first = ["", "First request incl. connection setup (bytes, request+response):", "",
             "| Payload | REST | gRPC |", "|---|---|---|"]
    for s in sorted({int(r["size"]) for r in rows if r["mode"] == "text"}):
        d = {r["proto"]: int(r["first_c2s"]) + int(r["first_s2c"]) for r in rows if r["mode"] == "text" and int(r["size"]) == s}
        first.append(f"| text {s} | {d['rest']} | {d['grpc']} |")
    return "\n".join(out + first)


def main():
    sizes = medians(load("latency_sizes.csv"))
    rps = medians(load("latency_rps.csv"))
    ramp = medians(load("latency_ramp.csv"))
    md = ["# Measured results (generated by scripts/make_report.py)", ""]
    if sizes:
        md += ["## Latency vs payload size (text body, 1 event, target 3000 rps)", "",
               table(sizes, [(p, "text", s, r) for s, r in SIZE_SHAPES for p in ("rest", "grpc")]), ""]
        chart_sizes(sizes)
    if rps:
        merged = dict(rps)
        merged.update({k: v for k, v in sizes.items() if k[1] == "text" and k[2] == 1024})
        keys = sorted({k for k in merged if k[1] == "text" and k[2] == 1024}, key=lambda k: (k[3], k[0]))
        md += ["## Latency vs load (1 KB payload)", "", table(merged, keys), ""]
        chart_rps(merged)
    if ramp:
        keys = sorted({k for k in ramp if k[1] == "text" and k[2] == 1024}, key=lambda k: (k[3], k[0]))
        md += ["## Capacity ramp (1 KB, --threads 512, all points kept including saturated)", "",
               table(ramp, keys), ""]
        chart_ramp(ramp)
    md += ["## On-wire bytes per request+response (steady state)", "", wire_table(), ""]
    chart_wire()
    open(os.path.join(RES, "summary.md"), "w").write("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main()
