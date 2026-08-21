#!/usr/bin/env python3
"""Fold the network-verification runs into one wide table: fat-tree size x implementation.

Every implementation solves the identical problem on the identical bytes, so the
correctness columns (`atoms`, `pairs`) are shared rather than per-implementation
-- a row where they disagree is a bug, not a result, and `check()` says so.
"""
import csv, os, statistics
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))

# display order == categorical slot order
IMPLS = [
    "jdd-bdd",
    "ndd",
    "gcflobdd/field-grouped",
    "gcflobdd/aligned-balanced",
    "gcflobdd/aligned-balanced-shared",
]
SHORT = {
    "jdd-bdd": "jdd",
    "ndd": "ndd",
    "gcflobdd/field-grouped": "gcf_fg",
    "gcflobdd/aligned-balanced": "gcf_ab",
    "gcflobdd/aligned-balanced-shared": "gcf_abs",
}
LABEL = {
    "jdd-bdd": "BDD (JDD)",
    "ndd": "NDD",
    "gcflobdd/field-grouped": "GCFLOBDD, field-grouped",
    "gcflobdd/aligned-balanced": "GCFLOBDD, aligned-balanced",
    "gcflobdd/aligned-balanced-shared": "GCFLOBDD, aligned-balanced-shared",
}


def rows(name="netverify.csv"):
    p = os.path.join(HERE, name)
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []


def num(r, key):
    v = r.get(key, "")
    if v in ("", "na", "-1", None):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def collect(name="netverify.csv"):
    """(k, impl) -> list of run dicts, ok runs only."""
    out = defaultdict(list)
    for r in rows(name):
        if r.get("status") != "ok":
            continue
        out[(int(r["k"]), r["impl"])].append(r)
    return out


def med(runs, key):
    vals = [num(r, key) for r in runs]
    vals = [v for v in vals if v is not None]
    return statistics.median(vals) if vals else None


def cell(data, k, impl, key):
    runs = data.get((k, impl))
    return med(runs, key) if runs else None


def fmt(v, nd=1):
    if v is None:
        return ""
    return f"{v:.{nd}f}" if nd else f"{int(round(v))}"


def check(data, ks):
    """Every implementation must agree on the answer. Returns a list of problems."""
    problems = []
    for k in ks:
        for key in ("atoms", "pairs", "predicates"):
            seen = {}
            for impl in IMPLS:
                v = cell(data, k, impl, key)
                if v is not None:
                    seen.setdefault(v, []).append(SHORT[impl])
            if len(seen) > 1:
                problems.append(f"k={k} {key}: " + "; ".join(
                    f"{int(v)} from {','.join(who)}" for v, who in seen.items()))
        # Sat-count digests are doubles; compare with a relative epsilon.
        for key in ("fwd_fraction", "acl_fraction"):
            vals = [(SHORT[i], cell(data, k, i, key)) for i in IMPLS]
            vals = [(n, v) for n, v in vals if v is not None]
            if len(vals) > 1:
                lo = min(v for _, v in vals)
                hi = max(v for _, v in vals)
                if lo > 0 and (hi - lo) / lo > 1e-9:
                    problems.append(f"k={k} {key}: spread {lo} .. {hi}")
    return problems


def main():
    data = collect()
    ks = sorted({k for k, _ in data})
    if not ks:
        print("no netverify.csv next to this script")
        return

    problems = check(data, ks)
    print("cross-implementation agreement:",
          "OK" if not problems else "FAILED")
    for p in problems:
        print("  !", p)

    head = ["k", "switches", "fib_rules", "acl_rules", "predicates", "atoms", "pairs", "seeds"]
    for impl in IMPLS:
        s = SHORT[impl]
        head += [f"{s}_ms", f"{s}_ap_ms", f"{s}_rss_kb", f"{s}_nodes", f"{s}_max_nodes"]
        if impl == "ndd":
            # NDD's size is NDD nodes plus the label BDDs underneath them.
            head.insert(head.index(f"{s}_max_nodes"), f"{s}_label_nodes")

    out = os.path.join(HERE, "netverify_all.csv")
    with open(out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(head)
        for k in ks:
            any_impl = next(i for i in IMPLS if (k, i) in data)
            seeds = len(data[(k, any_impl)])
            # k pods: k*k/2 edge + k*k/2 agg + (k/2)^2 core
            switches = k * k + (k // 2) ** 2
            row = [k, switches,
                   fmt(cell(data, k, any_impl, "fib_rules"), 0),
                   fmt(cell(data, k, any_impl, "acl_rule_count"), 0),
                   fmt(cell(data, k, any_impl, "predicates"), 0),
                   fmt(cell(data, k, any_impl, "atoms"), 0),
                   fmt(cell(data, k, any_impl, "pairs"), 0),
                   seeds]
            for impl in IMPLS:
                row += [fmt(cell(data, k, impl, "total_ms"), 1),
                        fmt(cell(data, k, impl, "ap_ms"), 1),
                        fmt(cell(data, k, impl, "peak_rss_kb"), 0),
                        fmt(cell(data, k, impl, "engine_nodes"), 0)]
                if impl == "ndd":
                    row += [fmt(cell(data, k, impl, "label_nodes"), 0)]
                row += [fmt(cell(data, k, impl, "max_nodes"), 0)]
            w.writerow(row)
    print("->", out)

    # A quick console view of the headline: total runtime, medians.
    print()
    print(f"{'k':>3} {'atoms':>7} " + " ".join(f"{SHORT[i]:>9}" for i in IMPLS))
    for k in ks:
        any_impl = next(i for i in IMPLS if (k, i) in data)
        line = f"{k:>3} {fmt(cell(data, k, any_impl, 'atoms'), 0):>7} "
        line += " ".join(f"{fmt(cell(data, k, i, 'total_ms'), 0):>9}" for i in IMPLS)
        print(line)


if __name__ == "__main__":
    main()
