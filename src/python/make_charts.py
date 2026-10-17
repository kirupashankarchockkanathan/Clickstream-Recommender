#!/usr/bin/env python3
"""Build report/slide charts from the real metric files in outputs/metrics (nothing is typed in by hand).
Run: python src/python/make_charts.py     (needs matplotlib only)
Colour: validated categorical order (blue, orange, aqua, yellow, magenta), one fixed colour per method; every bar is
direct-labelled with its value, so identity never depends on colour alone.
"""
import glob, json, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

M = "outputs/metrics"; OUT = "outputs/charts"; os.makedirs(OUT, exist_ok=True)
INK, MUTED, GRID, BG = "#0b0b0b", "#52514e", "#e6e5e1", "#fcfcfb"
COL = {"pop": "#2a78d6", "als_batch": "#eb6834", "als_foldin": "#1baf7a", "cooc": "#eda100", "hybrid_rrf": "#e87ba4"}
LABEL = {"pop": "Popularity", "als_batch": "ALS batch (stale)", "als_foldin": "ALS + session fold-in", "cooc": "Co-occurrence (kNN-style)", "hybrid_rrf": "Hybrid (RRF)"}
plt.rcParams.update({"font.family": "DejaVu Sans", "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "text.color": INK,
                     "xtick.color": MUTED, "ytick.color": MUTED, "axes.facecolor": BG, "figure.facecolor": BG, "font.size": 10})


def rows(path):
    return [json.loads(l) for l in open(path, encoding="utf8") if l.startswith("{")] if os.path.exists(path) else []


def clean(ax):
    for s in ("top", "right"): ax.spines[s].set_visible(False)
    ax.grid(axis="y", color=GRID, linewidth=0.8); ax.set_axisbelow(True)


def eval_chart():
    r = rows(f"{M}/eval_by_k_final.json")
    if not r: return
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=False)
    for ax, metric, title in zip(axes, ["HR_20", "NDCG_20"], ["Hit rate @20", "NDCG @20"]):
        order = ["pop", "als_batch", "als_foldin", "cooc", "hybrid_rrf"]; w = 0.16
        for j, m in enumerate(order):
            vals = [next(x[metric] for x in r if x["method"] == m and x["k"] == k) for k in (1, 2, 3)]
            xs = [i + (j - 2) * w for i in range(3)]
            ax.bar(xs, vals, width=w - 0.02, color=COL[m], label=LABEL[m])
            for x, v in zip(xs, vals): ax.text(x, v + 0.005, f"{v:.2f}", ha="center", fontsize=7, color=MUTED)
        ax.set_xticks(range(3)); ax.set_xticklabels(["after 1 click", "after 2 clicks", "after 3 clicks"])
        ax.set_title(title, loc="left", fontsize=11); clean(ax)
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, frameon=False, fontsize=8, loc="lower center", ncol=5)
    n = sum(x["cases"] for x in r if x["method"] == "pop")
    fig.suptitle(f"Next-item prediction on held-out sessions (Nov 21-30 2019, {n:,} cases, temporal split)", x=0.01, ha="left", fontsize=11)
    fig.tight_layout(rect=(0, 0.06, 1, 1)); fig.savefig(f"{OUT}/eval_methods.png", dpi=170); plt.close(fig)


def grid_chart():
    files = sorted(glob.glob(f"{M}/eval_by_k_val_*.json"))
    if not files: return
    data = []
    for f in files:
        name = os.path.basename(f)[len("eval_by_k_val_"):-5]
        r = [x for x in rows(f) if x["method"] == "als_foldin" and x["k"] == 1]
        if r: data.append((name, r[0]["NDCG_20"]))
    data.sort(key=lambda t: t[1])
    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    ax.barh([d[0].replace("_", " ").replace("r", "rank ", 1).replace(" a", " alpha ").replace(" l", " reg ") for d in data], [d[1] for d in data], color=COL["als_foldin"], height=0.6)
    for i, d in enumerate(data): ax.text(d[1] + 0.003, i, f"{d[1]:.3f}", va="center", fontsize=8, color=MUTED)
    ax.set_title("ALS fold-in tuning on the validation window (NDCG@20 after 1 click)", loc="left", fontsize=10)
    for s in ("top", "right"): ax.spines[s].set_visible(False)
    ax.grid(axis="x", color=GRID); ax.set_axisbelow(True); fig.tight_layout(); fig.savefig(f"{OUT}/als_grid.png", dpi=170); plt.close(fig)


def veracity_chart():
    r = rows(f"{M}/preprocess_veracity.json")
    if not r: return
    order = ["raw_rows", "after_required_fields", "after_valid_event_type", "after_valid_price", "after_dedup", "final_clean_rows"]
    d = {x["stage"]: x["rows"] for x in r}
    steps = [("missing key field", d[order[0]] - d[order[1]]), ("invalid event type", d[order[1]] - d[order[2]]),
             ("price <= 0 / null", d[order[2]] - d[order[3]]), ("duplicate rows", d[order[3]] - d[order[4]]), ("bot-like sessions", d[order[4]] - d[order[5]])]
    fig, ax = plt.subplots(figsize=(7.5, 3.4))
    ax.barh([s[0] for s in steps], [s[1] for s in steps], color=COL["als_batch"], height=0.55)
    for i, s in enumerate(steps): ax.text(s[1] + 3000, i, f"{s[1]:,}", va="center", fontsize=8, color=MUTED)
    ax.set_title(f"Veracity: {d[order[0]] - d[order[5]]:,} of {d[order[0]]:,} rows removed ({100*(d[order[0]]-d[order[5]])/d[order[0]]:.2f}%)", loc="left", fontsize=10)
    ax.invert_yaxis()
    for s in ("top", "right"): ax.spines[s].set_visible(False)
    ax.grid(axis="x", color=GRID); ax.set_axisbelow(True); fig.tight_layout(); fig.savefig(f"{OUT}/veracity.png", dpi=170); plt.close(fig)


def bench_chart():
    r = rows(f"{M}/tuning_bench.json")
    if not r: return
    groups = {}
    for x in r: groups.setdefault(x["experiment"], []).append(x)
    names = {"E1": "E1 JSON vs Parquet", "E2": "E2 shuffle partitions", "E3": "E3 caching", "E4": "E4 small files", "E5": "E5 join strategy", "E6": "E6 partition pruning"}
    fig, axes = plt.subplots(2, 3, figsize=(12, 6.2))
    for ax, (e, items) in zip(axes.ravel(), sorted(groups.items())):
        ax.barh([i["variant"] for i in items], [i["seconds"] for i in items], color=COL["pop"], height=0.55)
        for k, i in enumerate(items): ax.text(i["seconds"], k, f' {i["seconds"]:.1f}s', va="center", fontsize=8, color=MUTED)
        ax.set_title(names.get(e, e), loc="left", fontsize=10); ax.invert_yaxis(); ax.tick_params(labelsize=7)
        for s in ("top", "right"): ax.spines[s].set_visible(False)
        ax.grid(axis="x", color=GRID); ax.set_axisbelow(True)
    fig.tight_layout(); fig.savefig(f"{OUT}/tuning.png", dpi=170); plt.close(fig)


def latency_chart():
    f = f"{M}/recs_stream_sample_all.jsonl"
    if not os.path.exists(f): return
    lat = [(r["done_ms"] - r["emit_ms"]) / 1000 for r in rows(f)]
    srt = sorted(lat); q = lambda p: srt[int(p * (len(srt) - 1))]
    fig, ax = plt.subplots(figsize=(7.5, 3.6))
    ax.hist(lat, bins=40, color=COL["als_foldin"])
    for p, name in ((0.5, "p50"), (0.9, "p90"), (0.99, "p99")):
        ax.axvline(q(p), color=MUTED, linewidth=1, linestyle="--"); ax.text(q(p), ax.get_ylim()[1] * 0.92, f" {name} {q(p):.1f}s", fontsize=8, color=MUTED)
    ax.set_xlabel("seconds from click to recommendation (Kafka -> Spark -> Kafka, 5 s trigger, 1,500 events/s)")
    ax.set_title(f"Streaming recommendation latency ({len(lat):,} messages)", loc="left", fontsize=10); clean(ax)
    fig.tight_layout(); fig.savefig(f"{OUT}/stream_latency.png", dpi=170); plt.close(fig)


if __name__ == "__main__":
    for f in (eval_chart, grid_chart, veracity_chart, bench_chart, latency_chart): f()
    print(sorted(os.listdir(OUT)))
