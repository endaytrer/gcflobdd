#!/usr/bin/env python3
"""Fold every run CSV into one long table: algorithm x qubits x implementation."""
import csv, os, statistics
from collections import defaultdict

# The run CSVs sit next to this script.
HERE = os.path.dirname(os.path.abspath(__file__))
R = HERE

# implementation display order == categorical slot order
IMPLS = ["gcflobdd", "cflobdd", "cudd"]
# GHZ is the textbook state-vector circuit.  The reference implements GHZ only
# as a 2n-qubit operator held as a matrix, a strictly larger object, so the two
# sides of that row are not the same construction; BENCHMARKS.md says so before
# comparing them.
ALGOS = ["ghz_vec", "bv", "dj", "qft", "grover"]
ALGO_LABEL = {
    "ghz_vec": "GHZ", "bv": "Bernstein-Vazirani", "dj": "Deutsch-Jozsa",
    "qft": "QFT", "grover": "Grover",
}


def rows(name):
    p = os.path.join(R, name)
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []


def ms(r):
    """Each implementation's own internal timer, in ms."""
    if r.get("duration_us"):
        return int(r["duration_us"]) / 1000.0
    if r.get("duration_ms"):
        return float(r["duration_ms"])
    return None


def size(r):
    """Diagram size: nodes + edges, counted the way the reference C++ counts --
    two edges per connection plus the entries of every distinct return map.

    Both implementations report `total` in that one convention, so the column is
    comparable across them directly.  CUDD reports node counts only and is
    labelled as such.
    """
    return r.get("total")


# cell[(algo, qubits, impl)] = dict(ms=, size=, status=, n=, correct=)
cell = {}
raw = defaultdict(list)


def collect(src, algo_field, impl_of, algo_of=None):
    for r in src:
        impl = impl_of(r)
        if impl is None:
            continue
        algo = algo_of(r) if algo_of else r[algo_field]
        raw[(algo, int(r["qubits"]), impl)].append(r)


# --- the CFLOBDD pair -------------------------------------------------------
# For GHZ/BV/DJ/QFT the reference build is HEAD as-is; for Grover it is
# HEAD + PR #10, which is what BENCHMARKS.md times.  On this machine the two
# builds are indistinguishable, so the choice does not move any number.
def ladder_impl(r):
    return {"rust-bigint": "gcflobdd", "cpp": "cflobdd"}.get(r["impl"])


def ladder_algo(r):
    """Both implementations' GHZ lands in one row.

    They build different objects -- this crate the n-qubit state vector, the
    reference a 2n-qubit operator held as a matrix -- so `ghz-vec` here and
    `ghz` there are the two halves of the same row.
    """
    algo = r["algo"].replace("-", "_")
    return "ghz_vec" if algo == "ghz" else algo


def grover_impl(r):
    return {"rust-bigint": "gcflobdd", "cpp-fixed": "cflobdd"}.get(r["impl"])


collect(rows("ladder.csv"), "algo", ladder_impl, algo_of=ladder_algo)
collect(rows("qft.csv"), "algo", ladder_impl)
collect(rows("grover_compare.csv"), "algo", grover_impl, algo_of=lambda r: "grover")

# --- the CUDD control group -------------------------------------------------
# CUDD carries the state itself, so its GHZ lines up with the state-vector
# circuit rather than with the reference's 2n-qubit operator.
CUDD_ALGO = {"GHZ": "ghz_vec", "BV": "bv", "DJ": "dj", "grover": "grover", "fourier": "qft"}
for r in rows("cudd.csv"):
    algo = CUDD_ALGO.get(r["algo"])
    if algo is None:
        continue
    raw[(algo, int(r["qubits"]), "cudd")].append(dict(
        duration_ms=(float(r["time_s"]) * 1000) if r["time_s"] else "",
        duration_us="", total=r["nodes"], status=r["status"], correct="na",
        peak_rss_kb=r["peak_rss_kb"], wall_s=r["wall_s"]))

for key, rs in raw.items():
    ok = [r for r in rs if r["status"] == "ok"]
    times = [ms(r) for r in ok if ms(r) is not None]
    sizes = [int(size(r)) for r in ok if size(r)]
    corr = [r["correct"] for r in rs if r["correct"] not in ("na", "")]
    cell[key] = dict(
        ms=statistics.median(times) if times else None,
        size=statistics.median(sizes) if sizes else None,
        status="ok" if ok else (rs[0]["status"] if rs else "?"),
        n=len(rs),
        correct=(f"{sum(1 for c in corr if c == '1')}/{len(corr)}" if corr else "na"),
        rss=statistics.median([int(r["peak_rss_kb"]) for r in rs if r.get("peak_rss_kb")])
            if any(r.get("peak_rss_kb") for r in rs) else None,
    )


def fmt_ms(v):
    if v is None:
        return ""
    return f"{v:.4g}"


def fmt(v):
    return "" if v is None else f"{int(v)}"


if __name__ == "__main__":
    out = os.path.join(R, "all_results.csv")
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["algorithm", "qubits",
                    "gcflobdd_ms", "gcflobdd_size", "gcflobdd_correct",
                    "cflobdd_ms", "cflobdd_size", "cflobdd_correct",
                    "cudd_ms", "cudd_nodes", "cudd_status", "seeds"])
            # gcflobdd_size and cflobdd_size are both the reference's counting
            # convention; cudd_nodes is a node count, which is why it keeps its
            # own column name.
        for algo in ALGOS:
            qs = sorted({q for (a, q, i) in cell if a == algo})
            for q in qs:
                g = cell.get((algo, q, "gcflobdd"), {})
                c = cell.get((algo, q, "cflobdd"), {})
                u = cell.get((algo, q, "cudd"), {})
                w.writerow([
                    ALGO_LABEL[algo], q,
                    fmt_ms(g.get("ms")), fmt(g.get("size")), g.get("correct", ""),
                    fmt_ms(c.get("ms")), fmt(c.get("size")), c.get("correct", ""),
                    fmt_ms(u.get("ms")), fmt(u.get("size")),
                    u.get("status", "not run"), g.get("n", ""),
                ])
    print("wrote", out)
    n_rows = sum(1 for algo in ALGOS for _ in {q for (a, q, i) in cell if a == algo})
    print(f"{len(ALGOS)} algorithms, {n_rows} algorithm-size rows")
