#!/usr/bin/env bash
# Both diagrams, measured under both counting conventions, medians over seeds.
set -uo pipefail
# C: the reference built with the two-line instrumentation that also reports its
#    diagram under *this* crate's convention (one edge per connection, no return
#    maps), on stderr as `structuralConnections`.
# R: this crate's benchmark, which reports both conventions itself.
HERE=$(cd "$(dirname "$0")" && pwd)
C=${CPP_INSTR_BIN:?set CPP_INSTR_BIN to the instrumented reference binary}
R=${RUST_BIN:?set RUST_BIN to the quantum benchmark built from this crate}
S=${TMPDIR:-/tmp}/counting.$$
mkdir -p "$S"
trap 'rm -rf "$S"' EXIT
OUT=$HERE/counting_convention.csv
echo "algo,qubits,seeds,cpp_own,gcflobdd_cppconv,cpp_rustconv,gcflobdd_own" > "$OUT"

# NF guard: the accumulators start with a leading space, which would otherwise
# contribute an empty field that sorts as 0 and drags the median down.
median() { tr ' ' '\n' <<<"$1" | awk 'NF' | sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:int((a[NR/2]+a[NR/2+1])/2)}'; }

run() {                      # run <label> <cpp-test> <rust-test> <p> <seeds...>
  local a=$1 ct=$2 rt=$3 p=$4; shift 4
  local q=$((2 ** p)) co="" gc="" cr="" go="" n=0
  for s in "$@"; do
    n=$((n + 1))
    local out sc cn tot
    out=$("$C" "$ct" "$p" "$s" 2>"$S/.err" | grep "^Duration:")
    sc=$(grep -o "structuralConnections: [0-9]*" "$S/.err" | tail -1 | awk '{print $2}')
    cn=$(sed -n 's/.*nodeCount: \([0-9]*\).*/\1/p' <<<"$out")
    co+=" $(sed -n 's/.*totalCount: \([0-9]*\).*/\1/p' <<<"$out")"
    cr+=" $((cn + sc))"
    local rout
    rout=$("$R" "$rt" "$p" "$s" 2>/dev/null | grep "^Duration:")
    go+=" $(sed -n 's/.*totalCount: \([0-9]*\).*/\1/p' <<<"$rout")"
    gc+=" $(sed -n 's/.*cflobddConvTotal: \([0-9]*\).*/\1/p' <<<"$rout")"
  done
  local mco mgc mcr mgo
  mco=$(median "$co"); mgc=$(median "$gc"); mcr=$(median "$cr"); mgo=$(median "$go")
  echo "$a,$q,$n,$mco,$mgc,$mcr,$mgo" | tee -a "$OUT"
}

# GHZ compares the reference's construction on both sides -- `testGHZAlgoMatrix`
# here, not the textbook `testGHZAlgo`, which builds a smaller object.
run GHZ    testGHZAlgo     testGHZAlgoMatrix   11 1
run GHZ    testGHZAlgo     testGHZAlgoMatrix   16 1
run BV     testBVAlgo      testBVAlgo          11 1 2 3
run BV     testBVAlgo      testBVAlgo          14 1 2 3
run DJ     testDJAlgo      testDJAlgo          11 1 2 3
run DJ     testDJAlgo      testDJAlgo          14 1 2 3
run QFT    testQFT         testQFT              3 1 2 3
run QFT    testQFT         testQFT              4 1 2 3
run Grover testGroversAlgo testGroversAlgoBig   5 1 2 3
run Grover testGroversAlgo testGroversAlgoBig   9 1 2 3
echo; echo "wrote $OUT"
