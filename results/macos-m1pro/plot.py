#!/usr/bin/env python3
"""Line charts of runtime and diagram size against qubit count, one per algorithm.

Palette: categorical slots 1-3 of the validated default (validate_palette.js,
light mode, --pairs all: worst CVD dE 9.2, worst normal-vision dE 24.0, all PASS).
Aqua sits below 3:1 on the light surface, so the relief rule applies -- satisfied
by the accompanying all_results.csv table view, and reinforced here by giving
every series its own marker and dash pattern (secondary encoding, which also
carries the figure into greyscale print).
"""
import csv
import os
import statistics
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter

from build_tables import cell, ALGOS, ALGO_LABEL

# `ghz_vec` carries only a gcflobdd series -- the reference does not run that
# circuit -- so it belongs in the table, not in a grid whose panels compare
# three implementations.  It keeps the 2x3 grid at five panels plus a legend.
PLOT_ALGOS = [a for a in ALGOS if a != "ghz_vec"]

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "figures")
os.makedirs(OUT, exist_ok=True)

# --- design tokens ---------------------------------------------------------
SURFACE = "#ffffff"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#8a8981"
GRID = "#e4e3de"

SERIES = {                        # slot: colour, marker, dash
    "gcflobdd": dict(c="#2a78d6", m="o", ls="-",              label="gcflobdd (this crate)"),
    "cflobdd":  dict(c="#eb6834", m="s", ls=(0, (5, 2)),      label="CFLOBDD (reference C++)"),
    "cudd":     dict(c="#1baf7a", m="^", ls=(0, (1.6, 1.8)),  label="CUDD ADD (control)"),
}
ORDER = ["gcflobdd", "cflobdd", "cudd"]

# Where a series stopped, and why -- annotated rather than drawn, so a line
# never implies a measurement that does not exist.  Every one of these was
# observed: (text, dx, dy, ha, va).
STOPS = {
    ("ghz", "cudd"):       ("aborts at 512", 6, 5, "left", "bottom"),
    ("bv", "cudd"):        ("times out\nat 32", 6, -3, "left", "top"),
    ("dj", "cudd"):        ("times out\nat 32", 6, -3, "left", "top"),
    ("grover", "cudd"):    ("times out\nat 32", 6, -3, "left", "top"),
    ("qft", "cudd"):       ("times out\nat 32", 6, -3, "left", "top"),
    ("grover", "cflobdd"): ("SIGBUS at 1024", 6, 6, "left", "bottom"),
    ("qft", "cflobdd"):    ("no implementation\nfinishes 32 qubits", 7, -4, "left", "top"),
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


def series(algo, impl, field):
    # A log axis cannot show 0, and the reference's timer has 1 ms granularity
    # -- its QFT reports a literal 0 below that.  Drop those rather than invent
    # a floor for them; the caption says the floor is there.
    pts = sorted((q, cell[(a, q, i)][field])
                 for (a, q, i) in cell
                 if a == algo and i == impl
                 and cell[(a, q, i)][field] is not None
                 and cell[(a, q, i)][field] > 0)
    return [p[0] for p in pts], [p[1] for p in pts]


def style_axes(ax, ylabel, ylim):
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlim(1.4, 1.4e5)
    ax.set_ylim(*ylim)
    ax.grid(True, which="major", color=GRID, linewidth=0.6, zorder=0)
    ax.grid(True, which="minor", color=GRID, linewidth=0.3, alpha=0.6, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color(MUTED)
    ax.spines["bottom"].set_color(MUTED)
    ax.set_xticks([2, 2**4, 2**8, 2**12, 2**16])
    ax.set_xticklabels(["2", "16", "256", "4K", "64K"])
    ax.xaxis.set_minor_formatter(NullFormatter())
    ax.yaxis.set_major_locator(LogLocator(base=10, numticks=12))
    ax.yaxis.set_minor_formatter(NullFormatter())


def draw(ax, algo, field, annotate=True):
    for impl in ORDER:
        s = SERIES[impl]
        x, y = series(algo, impl, field)
        if not x:
            continue
        ax.plot(x, y, color=s["c"], linestyle=s["ls"], linewidth=1.6,
                marker=s["m"], markersize=4.2, markeredgecolor=SURFACE,
                markeredgewidth=0.8, zorder=3, clip_on=True)
        note = STOPS.get((algo, impl))
        if annotate and note:
            text, dx, dy, ha, va = note
            ax.annotate(text, xy=(x[-1], y[-1]), xytext=(dx, dy),
                        textcoords="offset points", fontsize=6,
                        color=MUTED, ha=ha, va=va, linespacing=1.2)


LEGEND = [Line2D([], [], color=SERIES[i]["c"], linestyle=SERIES[i]["ls"],
                 linewidth=1.6, marker=SERIES[i]["m"], markersize=4.2,
                 markeredgecolor=SURFACE, markeredgewidth=0.8,
                 label=SERIES[i]["label"]) for i in ORDER]

# The control group reaches 16-qubit QFT in 52.9 s, an order of magnitude above
# anything the two CFLOBDDs take, so the ceiling has to clear it or the point
# lands off the panel and takes its annotation with it.
TIME_LIM = (5e-3, 1.5e5)
SIZE_LIM = (3, 3e5)


def grid_figure(field, ylabel, ylim, title, subtitle, fname):
    fig, axes = plt.subplots(2, 3, figsize=(8.4, 5.0), sharex=True, sharey=True)
    fig.subplots_adjust(left=0.075, right=0.985, top=0.795, bottom=0.095,
                        wspace=0.13, hspace=0.30)
    flat = axes.flatten()
    for ax, algo in zip(flat, PLOT_ALGOS):
        draw(ax, algo, field)
        style_axes(ax, ylabel, ylim)
        ax.set_title(ALGO_LABEL[algo], color=INK, pad=5, fontweight="medium")
    # Hide x labels only where a panel sits directly below to carry them --
    # column 3 has no second row, so Deutsch-Jozsa keeps its own.  sharex hides
    # tick labels by default, so that panel has to opt back in explicitly.
    for i in range(3):
        if i + 3 < len(PLOT_ALGOS):
            flat[i].tick_params(labelbottom=False)
        else:
            flat[i].tick_params(labelbottom=True)
            flat[i].set_xlabel("qubits")
    for i, ax in enumerate(flat[:len(PLOT_ALGOS)]):
        if i % 3 == 0:
            ax.set_ylabel(ylabel)
    for ax in flat[3:len(PLOT_ALGOS)]:
        ax.set_xlabel("qubits")
    flat[len(PLOT_ALGOS)].axis("off")
    flat[len(PLOT_ALGOS)].legend(handles=LEGEND, loc="center left", fontsize=8,
                            handlelength=2.6, labelspacing=0.9,
                            bbox_to_anchor=(-0.02, 0.62))
    fig.text(0.075, 0.958, title, fontsize=12, color=INK, fontweight="bold", ha="left")
    fig.text(0.075, 0.918, subtitle, fontsize=7.6, color=INK_2, ha="left",
             va="top", linespacing=1.45)
    for ext in ("svg", "pdf", "png"):
        fig.savefig(os.path.join(OUT, f"{fname}.{ext}"), dpi=200)
    plt.close(fig)
    print("wrote", fname, "(svg, pdf, png)")


def single(algo, field, ylabel, ylim, title, fname):
    fig, ax = plt.subplots(figsize=(4.2, 3.1))
    fig.subplots_adjust(left=0.165, right=0.97, top=0.87, bottom=0.155)
    draw(ax, algo, field)
    style_axes(ax, ylabel, ylim)
    ax.set_xlabel("qubits")
    ax.set_ylabel(ylabel)
    ax.set_title(title, color=INK, pad=7, fontweight="medium", loc="left")
    ax.legend(handles=LEGEND, fontsize=6.8, handlelength=2.4,
              loc="lower right", labelspacing=0.5)
    for ext in ("svg", "pdf", "png"):
        fig.savefig(os.path.join(OUT, f"{fname}.{ext}"), dpi=200)
    plt.close(fig)


# --- qubit counts that are not powers of two -------------------------------
#
# A second pair of figures, this crate only: the reference is indexed by level,
# so 2^p is the only register it can build and there is nothing to compare
# against.  Two series per panel, split on whether the count is a power of two,
# because the question the figure answers is whether the uneven grammars sit on
# the same curve as the even ones.

UNEVEN_ALGOS = ["ghz", "bv", "dj", "grover", "qft"]
UNEVEN_TEST = {"ghz": "ghz", "bv": "bv", "dj": "dj", "grover": "grover", "qft": "qft"}

EVEN_STYLE = dict(c="#2a78d6", m="o", ls="-", label="power of two (balanced grammar)")
ODD_STYLE = dict(c="#eb6834", m="D", ls=(0, (5, 2)), label="any other count (uneven grammar)")

UNEVEN_LEGEND = [Line2D([], [], color=st["c"], linestyle=st["ls"], linewidth=1.6,
                        marker=st["m"], markersize=4.4, markeredgecolor=SURFACE,
                        markeredgewidth=0.8, label=st["label"])
                 for st in (EVEN_STYLE, ODD_STYLE)]


def uneven_points():
    """{algo: {qubits: (median ms, median size)}} from uneven.csv."""
    by = defaultdict(list)
    with open(os.path.join(HERE, "uneven.csv")) as fh:
        for r in csv.DictReader(fh):
            assert r["status"] == "ok" and r["correct"] == "1", r
            by[(r["algo"], int(r["qubits"]))].append(r)
    out = defaultdict(dict)
    for (algo, n), rs in by.items():
        out[algo][n] = (statistics.median(int(r["duration_us"]) for r in rs) / 1000,
                        statistics.median(int(r["conv_total"]) for r in rs))
    return out


def uneven_figure(index, ylabel, title, subtitle, fname):
    pts = uneven_points()
    fig, axes = plt.subplots(2, 3, figsize=(8.4, 5.0))
    fig.subplots_adjust(left=0.075, right=0.90, top=0.795, bottom=0.095,
                        wspace=0.42, hspace=0.62)
    flat = axes.flatten()
    for ax, algo in zip(flat, UNEVEN_ALGOS):
        data = pts[UNEVEN_TEST[algo]]
        for style, keep in ((EVEN_STYLE, lambda n: n & (n - 1) == 0),
                            (ODD_STYLE, lambda n: n & (n - 1) != 0)):
            xy = sorted((n, v[index]) for n, v in data.items() if keep(n))
            if not xy:
                continue
            ax.plot([p[0] for p in xy], [p[1] for p in xy], color=style["c"],
                    linestyle=style["ls"], linewidth=1.5, marker=style["m"],
                    markersize=4.4, markeredgecolor=SURFACE, markeredgewidth=0.8,
                    zorder=3)
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.grid(True, which="major", color=GRID, linewidth=0.6, zorder=0)
        ax.grid(True, which="minor", color=GRID, linewidth=0.3, alpha=0.6, zorder=0)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        # x ticks at the powers of two the panel actually covers, so the
        # even/uneven distinction the figure is about is legible on the axis.
        pows = sorted(n for n in data if n & (n - 1) == 0)
        ax.set_xticks(pows)
        ax.set_xticklabels([f"{n // 1024:,}K" if n >= 1024 else f"{n:,}" for n in pows])
        ax.xaxis.set_minor_formatter(NullFormatter())
        # Several of these panels span less than a decade, where the default
        # decade-only locator leaves the y axis unlabelled; QFT spans five, where
        # subdividing every decade would bury it.  Pick on the span.
        lo, hi = ax.get_ylim()
        subs = (1,) if (hi / lo) > 40 else (1, 2, 3, 5)
        ax.yaxis.set_major_locator(LogLocator(base=10, subs=subs, numticks=20))
        # Runtime panels drop below 1 ms, where a whole-number format prints 0.
        ax.yaxis.set_major_formatter(
            FuncFormatter(lambda v, _: f"{v:,.0f}" if v >= 1 else f"{v:g}"))
        ax.yaxis.set_minor_formatter(NullFormatter())
        ax.set_title(ALGO_LABEL[algo], color=INK, pad=5, fontweight="medium")
        ax.set_xlabel("qubits")
        ax.set_ylabel(ylabel)
    flat[len(UNEVEN_ALGOS)].axis("off")
    flat[len(UNEVEN_ALGOS)].legend(handles=UNEVEN_LEGEND, loc="center left",
                                   fontsize=7.6, handlelength=2.4, labelspacing=0.9,
                                   bbox_to_anchor=(-0.08, 0.62))
    fig.text(0.075, 0.958, title, fontsize=12, color=INK, fontweight="bold", ha="left")
    fig.text(0.075, 0.918, subtitle, fontsize=7.6, color=INK_2, ha="left",
             va="top", linespacing=1.45)
    for ext in ("svg", "pdf", "png"):
        fig.savefig(os.path.join(OUT, f"{fname}.{ext}"), dpi=200)
    plt.close(fig)
    print("wrote", fname, "(svg, pdf, png)")


if __name__ == "__main__":
    grid_figure("ms", "runtime (ms)", TIME_LIM,
                "Runtime against qubit count",
                "Each implementation's own timer, log-log; medians over available seeds. "
                "The reference's timer floors at 1 ms.\n"
                "Apple M1 Pro, macOS 26.5.2, 10 cores, 32 GB; strictly sequential runs.",
                "runtime_all")

    grid_figure("size", "diagram size", SIZE_LIM,
                "Diagram size against qubit count",
                "Nodes + edges of the result diagram in the reference C++'s counting convention, log-log.\n"
                "CUDD counts nodes only, so the control group is measured favourably.",
                "size_all")

    for algo in PLOT_ALGOS:
        single(algo, "ms", "runtime (ms)", TIME_LIM,
               f"{ALGO_LABEL[algo]} — runtime", f"runtime_{algo}")
        single(algo, "size", "diagram size", SIZE_LIM,
               f"{ALGO_LABEL[algo]} — diagram size", f"size_{algo}")
    print("wrote 10 per-algorithm figures (svg, pdf) to", OUT)

    uneven_figure(0, "runtime (ms)",
                  "Runtime at qubit counts that are not powers of two",
                  "This crate only -- the reference is indexed by level and can build no other register. "
                  "Medians over 3 seeds\nwhere the algorithm takes one; log-log. "
                  "QFT and Grover need an even count, so they carry no odd points.",
                  "uneven_runtime")

    uneven_figure(1, "diagram size",
                  "Diagram size at qubit counts that are not powers of two",
                  "Nodes + edges in the reference's counting convention, log-log. "
                  "An uneven count carries two adjacent block\nsizes per level where a power of two carries one, "
                  "which is the whole of the difference between the curves.",
                  "uneven_size")
