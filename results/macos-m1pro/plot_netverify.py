#!/usr/bin/env python3
"""Runtime, memory and diagram size against fat-tree size, one implementation per series.

Palette: categorical slots 1-5 of the validated default (validate_palette.js,
light mode, default adjacent pairlist, which is the line-chart case). Three of
the five light-mode slots sit below 3:1 on the light surface, so the relief rule
applies -- satisfied by the accompanying netverify_all.csv table view, and
reinforced here by giving every series its own marker and dash pattern, which
also carries the figures into greyscale print.

Not every engine reports every size. JDD keeps its live-node count private and
NDD has no per-diagram walk, so the size panels carry only the series that can
honestly fill them, and say so on the figure rather than leaving a gap a reader
would read as zero.
"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter

from build_netverify_tables import IMPLS, LABEL, collect, cell

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "figures")
os.makedirs(OUT, exist_ok=True)

# --- design tokens (shared with plot.py) -----------------------------------
SURFACE = "#ffffff"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#8a8981"
GRID = "#e4e3de"

SERIES = {
    "jdd-bdd":                          dict(c="#2a78d6", m="o", ls="-",             z=5),
    "ndd":                              dict(c="#eb6834", m="s", ls=(0, (5, 2)),     z=4),
    "gcflobdd/field-grouped":           dict(c="#1baf7a", m="^", ls=(0, (1.6, 1.8)), z=3),
    "gcflobdd/aligned-balanced":        dict(c="#eda100", m="D", ls=(0, (4, 1.5, 1, 1.5)), z=2),
    "gcflobdd/aligned-balanced-shared": dict(c="#e87ba4", m="v", ls=(0, (3, 1, 1, 1, 1, 1)), z=1),
}

plt.rcParams.update({
    "font.family": ["DejaVu Sans"],
    "font.size": 8.5,
    "axes.titlesize": 9.5,
    "axes.labelsize": 8.5,
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "axes.edgecolor": MUTED,
    "axes.linewidth": 0.7,
    "axes.labelcolor": INK_2,
    "text.color": INK,
    "xtick.color": INK_2,
    "ytick.color": INK_2,
    "xtick.labelsize": 7.5,
    "ytick.labelsize": 7.5,
    "xtick.major.width": 0.7,
    "ytick.major.width": 0.7,
    "xtick.major.size": 3,
    "ytick.major.size": 3,
    "legend.frameon": False,
    "svg.fonttype": "none",
    "pdf.fonttype": 42,
})

DATA = collect()
KS = sorted({k for k, _ in DATA})


def points(impl, field):
    pts = [(k, cell(DATA, k, impl, field)) for k in KS]
    pts = [(k, v) for k, v in pts if v is not None and v > 0]
    return [p[0] for p in pts], [p[1] for p in pts]


def style(ax, ylabel, title, subtitle):
    ax.set_yscale("log")
    ax.set_xlabel("fat-tree size $k$  (switches $\\propto k^2$)", labelpad=6)
    ax.set_ylabel(ylabel)
    ax.set_xticks(KS)
    ax.set_xticklabels([str(k) for k in KS])
    ax.grid(True, which="major", color=GRID, linewidth=0.6, zorder=0)
    ax.grid(True, which="minor", color=GRID, linewidth=0.3, alpha=0.6, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color(MUTED)
    ax.spines["bottom"].set_color(MUTED)
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0), numticks=16))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}" if v >= 1 else f"{v:g}"))
    lines = subtitle if isinstance(subtitle, (list, tuple)) else [subtitle]
    ax.set_title(title, loc="left", pad=11 + 11 * len(lines), color=INK,
                 fontweight="medium")
    for n, line in enumerate(reversed(lines)):
        ax.text(0, 1.012 + 0.052 * n, line, transform=ax.transAxes,
                fontsize=7.8, color=INK_2, va="bottom")


def figure(field, ylabel, title, subtitle, fname, impls, note=None):
    fig, ax = plt.subplots(figsize=(7.4, 4.9))
    drawn = []
    for impl in impls:
        s = SERIES[impl]
        x, y = points(impl, field)
        if not x:
            continue
        ax.plot(x, y, color=s["c"], marker=s["m"], linestyle=s["ls"],
                linewidth=2, markersize=5, markeredgecolor=SURFACE,
                markeredgewidth=1.2, zorder=10 + s["z"], label=LABEL[impl])
        drawn.append(impl)
    style(ax, ylabel, title, subtitle)
    if note:
        ax.text(0.99, 0.02, note, transform=ax.transAxes, fontsize=7,
                color=MUTED, ha="right", va="bottom")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.13), ncol=2,
              fontsize=7.6, labelcolor=INK_2, handlelength=2.6,
              columnspacing=2.4, borderaxespad=0)
    fig.tight_layout()
    for ext in ("png", "pdf", "svg"):
        fig.savefig(os.path.join(OUT, f"{fname}.{ext}"), dpi=200)
    plt.close(fig)
    print("->", os.path.join(OUT, fname), "with", len(drawn), "series")


def main():
    if not KS:
        print("no netverify.csv next to this script")
        return

    atoms = {k: cell(DATA, k, IMPLS[0], "atoms") for k in KS}
    span = f"{int(atoms[KS[0]]):,} atoms at k={KS[0]} to {int(atoms[KS[-1]]):,} at k={KS[-1]}"

    figure("total_ms", "total time (ms, log)",
           "Network verification, end to end",
           ["forwarding and ACL predicates, atomic predicates, all-pairs reachability",
            f"median of 3 seeds; all five return the identical answer ({span})"],
           "netverify_runtime", IMPLS,
           note="the two aligned-balanced grammars coincide to within 2.1%")

    figure("ap_ms", "atomic-predicate time (ms, log)",
           "Atomic predicates alone",
           ["the stage that dominates, and the one that leans hardest",
            "on canonical equality of handles"],
           "netverify_ap", IMPLS)

    figure("peak_rss_kb", "peak RSS (kB, log)",
           "Peak resident memory",
           ["whole process, so every series carries the same JVM;",
            "GCFLOBDD's nodes live outside the Java heap but inside this number"],
           "netverify_memory", IMPLS)

    size_impls = [i for i in IMPLS if i != "ndd"]
    figure("max_nodes", "nodes in the largest predicate (log)",
           "Diagram size",
           ["the largest single predicate built during the run;",
            "GCFLOBDD counts one node per grouping, which is not a BDD node"],
           "netverify_size", size_impls,
           note="NDD has no per-diagram node walk, so it cannot appear here")


if __name__ == "__main__":
    main()
