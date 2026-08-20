#!/usr/bin/env bash
#
# Run this crate's quantum benchmarks and the reference C++ CFLOBDD's over the
# same algorithm/size grid, and write one CSV row per run.
#
# Both binaries take `<test> <p> [seed]` (qubits = 2^p) and print the same
# summary line, so the same parser serves both.
#
# Usage:
#   scripts/compare_cflobdd.sh                  # full grid
#   scripts/compare_cflobdd.sh ghz bv           # only these algorithms
#   PMAX=6 TIMEOUT=120 scripts/compare_cflobdd.sh
#
# Environment:
#   CPP_BIN   path to the reference binary   (default ../cflobdd/CFLOBDD/cflobdd)
#   RUST_BIN  path to this crate's benchmark (default: newest target/release build)
#   OUT       output CSV                     (default results/quantum_compare.csv)
#   LABEL     name for this crate's rows     (default rust; e.g. rust-bigint)
#   CPP_LABEL name for the reference's rows  (default cpp; e.g. cpp-fixed)
#   ONLY      rust | cpp | both              (default both)
#   APPEND    1 to add to an existing CSV rather than start one
set -uo pipefail

here=$(cd "$(dirname "$0")/.." && pwd)
CPP_BIN=${CPP_BIN:-$here/../cflobdd/CFLOBDD/cflobdd}
RUST_BIN=${RUST_BIN:-}
OUT=${OUT:-$here/results/quantum_compare.csv}
LABEL=${LABEL:-rust}
CPP_LABEL=${CPP_LABEL:-cpp}
ONLY=${ONLY:-both}
APPEND=${APPEND:-0}
TIMEOUT=${TIMEOUT:-300}
PMIN=${PMIN:-1}
PMAX=${PMAX:-8}
SEEDS=${SEEDS:-"1 2 3"}

if [ -z "$RUST_BIN" ]; then
  cargo build --release --test quantum --manifest-path "$here/Cargo.toml" >/dev/null 2>&1
  RUST_BIN=$(ls -t "$here"/target/release/deps/quantum-* 2>/dev/null | grep -v '\.d$' | head -1)
fi
[ "$ONLY" = cpp  ] || [ -x "$RUST_BIN" ] || { echo "error: rust benchmark not built" >&2; exit 1; }
[ "$ONLY" = rust ] || [ -x "$CPP_BIN"  ] || { echo "error: $CPP_BIN not built" >&2; exit 1; }

mkdir -p "$(dirname "$OUT")"
if [ "$APPEND" != 1 ] || [ ! -s "$OUT" ]; then
  echo "impl,algo,p,qubits,seed,wall_s,peak_rss_kb,duration_ms,duration_us,nodes,edges,total,correct,verified,status" > "$OUT"
fi

# Skip a side entirely when ONLY selects the other one.
run_rust() { [ "$ONLY" = cpp  ] || run "$LABEL" "$RUST_BIN" "$@"; }
run_cpp()  { [ "$ONLY" = rust ] || run "$CPP_LABEL" "$CPP_BIN"  "$@"; }

tmp_out=$(mktemp); tmp_time=$(mktemp)
trap 'rm -f "$tmp_out" "$tmp_time"' EXIT

# run <impl> <binary> <algo-label> <test-name> <p> [seed]
run() {
  local impl=$1 bin=$2 label=$3 test=$4 p=$5 seed=${6:-}
  local qubits=$((2 ** p))

  /usr/bin/time -f '%e %M' -o "$tmp_time" \
    timeout "$TIMEOUT" "$bin" "$test" "$p" $seed > "$tmp_out" 2>/dev/null
  local rc=$?

  local status=ok
  if [ $rc -eq 124 ]; then status=timeout
  elif [ $rc -ne 0 ]; then status="exit$rc"; fi

  local wall rss
  read -r wall rss < <(tail -n 1 "$tmp_time" 2>/dev/null) || { wall=; rss=; }
  case "$wall" in ''|*[!0-9.]*) wall=; rss=;; esac

  # Three correctness spellings, matching the reference harness's outputs.
  local correct=na
  if   grep -q "^equal: "      "$tmp_out"; then correct=$(grep -m1 "^equal: "      "$tmp_out" | awk '{print $2}')
  elif grep -q "^is same: "    "$tmp_out"; then correct=$(grep -m1 "^is same: "    "$tmp_out" | awk '{print $3}')
  elif grep -q "^is_correct: " "$tmp_out"; then correct=$(grep -m1 "^is_correct: " "$tmp_out" | awk '{print $2}')
  fi

  # Grover here also checks the *whole* state against theory, not just that the
  # peak landed on the right string -- a partially amplified state still peaks
  # correctly, which is how the reference's failure hides. `na` for everything
  # else, including every reference run.
  # `matches theory: na` means the run was told not to verify, so leave the
  # column at na rather than recording an empty field.
  local verified=na
  if grep -q "matches theory: [01]" "$tmp_out"; then
    verified=$(grep -m1 -o "matches theory: [01]" "$tmp_out" | awk '{print $3}')
  fi

  local dur= us= nodes= edges= total=
  if grep -q "^Duration: " "$tmp_out"; then
    local line; line=$(grep -m1 "^Duration: " "$tmp_out")
    dur=$(  sed -n 's/.*Duration: \([0-9]*\).*/\1/p'   <<<"$line")
    nodes=$(sed -n 's/.*nodeCount: \([0-9]*\).*/\1/p'  <<<"$line")
    # the reference misspells it "egdeCount" in GHZ; accept either
    edges=$(sed -n 's/.*e[dg]*[gd]eCount: \([0-9]*\).*/\1/p' <<<"$line")
    total=$(sed -n 's/.*totalCount: \([0-9]*\).*/\1/p' <<<"$line")
    # only this crate's binary reports microseconds
    us=$(   sed -n 's/.*durationUs: \([0-9]*\).*/\1/p'  <<<"$line")
  fi

  echo "$impl,$label,$p,$qubits,${seed:-na},$wall,$rss,$dur,$us,$nodes,$edges,$total,$correct,$verified,$status" >> "$OUT"
  printf '%-5s %-4s p=%-2s q=%-4s seed=%-3s %-8s correct=%-3s verified=%-3s %sus (wall %ss)\n' \
    "$impl" "$label" "$p" "$qubits" "${seed:-na}" "$status" "$correct" "$verified" "${us:-${dur:-?}000}" "${wall:-?}"

  [ "$status" = ok ]
}

# Which algorithms to run, and how each is invoked.
want() { [ $# -eq 0 ] && return 0; local a=$1; shift; for w in "$@"; do [ "$w" = "$a" ] && return 0; done; return 1; }
ALGOS=("$@")

if want ghz "${ALGOS[@]}"; then
  for p in $(seq "$PMIN" "$PMAX"); do
    rust_ok=1; cpp_ok=1
    [ ${RUST_DONE_ghz:-0} -eq 1 ] || run_rust ghz testGHZAlgo "$p" || rust_ok=0
    [ ${CPP_DONE_ghz:-0}  -eq 1 ] || run_cpp  ghz testGHZAlgo "$p" || cpp_ok=0
    [ $rust_ok -eq 1 ] || RUST_DONE_ghz=1
    [ $cpp_ok  -eq 1 ] || CPP_DONE_ghz=1
    [ "$ONLY" = rust ] && CPP_DONE_ghz=1
    [ "$ONLY" = cpp  ] && RUST_DONE_ghz=1
    [ ${RUST_DONE_ghz:-0} -eq 1 ] && [ ${CPP_DONE_ghz:-0} -eq 1 ] && break
  done
fi

for spec in "bv testBVAlgo $PMAX" "dj testDJAlgo $PMAX" "qft testQFT 6"; do
  set -- $spec
  algo=$1 test=$2 cap=$3
  want "$algo" "${ALGOS[@]}" || continue
  rust_done=0; cpp_done=0
  [ "$ONLY" = rust ] && cpp_done=1
  [ "$ONLY" = cpp  ] && rust_done=1
  for p in $(seq "$PMIN" "$(( PMAX < cap ? PMAX : cap ))"); do
    for s in $SEEDS; do
      [ $rust_done -eq 1 ] || run_rust "$algo" "$test" "$p" "$s" || rust_done=1
      [ $cpp_done  -eq 1 ] || run_cpp  "$algo" "$test" "$p" "$s" || cpp_done=1
    done
    [ $rust_done -eq 1 ] && [ $cpp_done -eq 1 ] && break
  done
done

# --- Grover -----------------------------------------------------------------
# The one algorithm whose cost is exponential whatever the representation, so
# the ladder is short and driven by GROVER_PMAX rather than PMAX.
#
# GROVER_MODE picks this crate's variant, since the two implementations do not
# agree on how to reach M^k:
#   testGroversAlgo      honest iteration, f64
#   testGroversAlgoFast  operator exponentiation, f64        (the reference's shape)
#   testGroversAlgoBig   operator exponentiation, wide float (the default here)
#
# A wrong answer does *not* stop the ladder: for the reference it is the result
# being measured, so the loop only stops on a run that fails to finish.
if want grover "${ALGOS[@]}"; then
  GROVER_MODE=${GROVER_MODE:-testGroversAlgoBig}
  case $GROVER_MODE in
    testGroversAlgo)     rust_label=grover-iterate ;;
    testGroversAlgoFast) rust_label=grover-fast ;;
    *)                   rust_label=grover-big ;;
  esac
  cap=${GROVER_PMAX:-6}
  rust_done=0; cpp_done=0
  [ "$ONLY" = rust ] && cpp_done=1
  [ "$ONLY" = cpp  ] && rust_done=1
  for p in $(seq "$PMIN" "$(( PMAX < cap ? PMAX : cap ))"); do
    for s in $SEEDS; do
      # `run` only reports failure for a run that did not finish, so a wrong
      # answer carries on to the next size, as it must.
      [ $rust_done -eq 1 ] || run_rust "$rust_label" "$GROVER_MODE" "$p" "$s" || rust_done=1
      [ $cpp_done  -eq 1 ] || run_cpp  grover testGroversAlgo "$p" "$s" || cpp_done=1
    done
    [ $rust_done -eq 1 ] && [ $cpp_done -eq 1 ] && break
  done
fi

echo
echo "wrote $OUT"
