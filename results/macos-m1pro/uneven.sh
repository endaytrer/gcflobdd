#!/usr/bin/env bash
#
# Qubit counts that are not powers of two.
#
# The reference C++ CFLOBDD takes a level, so its register is always 2^p
# variables; this crate takes the count itself (`qN`) and builds an
# aligned-balanced grammar over it, which divides unevenly whenever N is not a
# power of two.  This sweep runs the same five algorithms over counts that
# bracket the powers of two, so an uneven count can be read against the even
# ones on either side of it.
#
# Usage:  BIN=<bigint build> OUT=results/macos-m1pro/uneven.csv uneven.sh [algo...]
set -uo pipefail

here=$(cd "$(dirname "$0")/../.." && pwd)
BIN=${BIN:-}
OUT=${OUT:-$here/results/uneven.csv}
TIMEOUT=${TIMEOUT:-300}
SEEDS=${SEEDS:-"1 2 3"}
# GHZ takes no seed, so it repeats the same run instead.  Same sample count as
# the seeded ladders, so every median in the table rests on the same n.
REPS=${REPS:-3}

if [ -z "$BIN" ]; then
  cargo build --release --test quantum --features bigint --manifest-path "$here/Cargo.toml" >/dev/null 2>&1
  BIN=$(ls -t "$here"/target/release/deps/quantum-* 2>/dev/null | grep -v '\.d$' | head -1)
fi
[ -x "$BIN" ] || { echo "error: quantum benchmark not built" >&2; exit 1; }

mkdir -p "$(dirname "$OUT")"
echo "algo,qubits,seed,wall_s,peak_rss_kb,duration_us,nodes,edges,total,correct,verified,status" > "$OUT"

tmp_out=$(mktemp); tmp_time=$(mktemp)
trap 'rm -f "$tmp_out" "$tmp_time"' EXIT

if /usr/bin/time -f '%e %M' -o /dev/null true >/dev/null 2>&1; then
  TIME_ARGS=(-f '%e %M'); TIME_STYLE=gnu
else
  TIME_ARGS=(-l); TIME_STYLE=bsd
fi
TIMEOUT_BIN=$(command -v timeout || command -v gtimeout || true)

# Stand-in for coreutils `timeout`, as in scripts/compare_cflobdd.sh: `set -m`
# gives the child its own process group so the kill reaches the measured binary
# and not just the `time` wrapper around it.
sh_timeout() {
  local secs=$1; shift
  local tmp fired; tmp=$(mktemp -t shto); fired=$tmp.fired; rm -f "$fired"
  set -m
  "$@" & local pid=$!
  ( sleep "$secs"; : > "$fired"; kill -TERM -- -"$pid" 2>/dev/null
    sleep 2; kill -KILL -- -"$pid" 2>/dev/null ) 2>/dev/null & local watch=$!
  local rc=0
  wait "$pid" 2>/dev/null || rc=$?
  [ -e "$fired" ] && rc=124
  kill -KILL -- -"$watch" 2>/dev/null; wait "$watch" 2>/dev/null
  kill -KILL -- -"$pid" 2>/dev/null
  rm -f "$tmp" "$fired"
  set +m
  return $rc
}

# run <algo-label> <test-name> <qubits> [seed] [check]
#
# Grover's `theory` check is f64 arithmetic and refuses past 2046 qubits, so
# larger counts ask for `answer`: the peak still has to land on the planted
# string, only the whole-state comparison is dropped.
run() {
  local label=$1 test=$2 n=$3 seed=${4:-} check=${5:-}

  local rc=0
  if [ -n "$TIMEOUT_BIN" ]; then
    /usr/bin/time "${TIME_ARGS[@]}" -o "$tmp_time" \
      "$TIMEOUT_BIN" "$TIMEOUT" "$BIN" "$test" "q$n" $seed $check > "$tmp_out" 2>/dev/null
    rc=$?
  else
    sh_timeout "$TIMEOUT" \
      /usr/bin/time "${TIME_ARGS[@]}" -o "$tmp_time" "$BIN" "$test" "q$n" $seed $check \
      > "$tmp_out" 2>/dev/null
    rc=$?
  fi

  local status=ok
  if [ $rc -eq 124 ]; then status=timeout
  elif [ $rc -ne 0 ]; then status="exit$rc"; fi

  local wall rss
  if [ "$TIME_STYLE" = gnu ]; then
    read -r wall rss < <(tail -n 1 "$tmp_time" 2>/dev/null) || { wall=; rss=; }
  else
    wall=$(awk '/ real /{print $1; exit}' "$tmp_time" 2>/dev/null)
    rss=$(awk '/maximum resident set size/{printf "%d", $1/1024; exit}' "$tmp_time" 2>/dev/null)
  fi
  case "$wall" in ''|*[!0-9.]*) wall=; rss=;; esac

  local correct=na
  if   grep -q "^equal: "      "$tmp_out"; then correct=$(grep -m1 "^equal: "      "$tmp_out" | awk '{print $2}')
  elif grep -q "^is same: "    "$tmp_out"; then correct=$(grep -m1 "^is same: "    "$tmp_out" | awk '{print $3}')
  elif grep -q "^is_correct: " "$tmp_out"; then correct=$(grep -m1 "^is_correct: " "$tmp_out" | awk '{print $2}')
  fi
  local verified=na
  if grep -q "matches theory: [01]" "$tmp_out"; then
    verified=$(grep -m1 -o "matches theory: [01]" "$tmp_out" | awk '{print $3}')
  fi

  local us= nodes= edges= total=
  if grep -q "^Duration: " "$tmp_out"; then
    local line; line=$(grep -m1 "^Duration: " "$tmp_out")
    nodes=$(sed -n 's/.*nodeCount: \([0-9]*\).*/\1/p'  <<<"$line")
    edges=$(sed -n 's/.*edgeCount: \([0-9]*\).*/\1/p'  <<<"$line")
    total=$(sed -n 's/.*totalCount: \([0-9]*\).*/\1/p' <<<"$line")
    us=$(   sed -n 's/.*durationUs: \([0-9]*\).*/\1/p' <<<"$line")
  fi

  echo "$label,$n,${seed:-na},$wall,$rss,$us,$nodes,$edges,$total,$correct,$verified,$status" >> "$OUT"
  printf '%-9s q=%-5s seed=%-3s %-8s correct=%-3s verified=%-3s %sus (wall %ss)\n' \
    "$label" "$n" "${seed:-na}" "$status" "$correct" "$verified" "${us:-?}" "${wall:-?}"
  [ "$status" = ok ]
}

want() { [ ${#ALGOS[@]} -eq 0 ] && return 0; local a=$1; for w in "${ALGOS[@]}"; do [ "$w" = "$a" ] && return 0; done; return 1; }
ALGOS=("$@")

# Counts bracketing the powers of two.  Grover and QFT need an even count --
# sqrt(N) = 2^(n/2) runs through Grover's iteration count and QFT's
# normalisation, and both amplitude types carry an integer exponent -- so the
# odd counts run only where the algorithm's own arithmetic permits them.
EVEN=${EVEN:-"100 128 200 256 300 500 512 1000 1024 2000 2048"}
ODD=${ODD:-"101 333 999"}
QFT_N=${QFT_N:-"6 8 10 12 14 16 18 20 22"}

# One ladder, stopping the moment a size fails.
ladder() {  # ladder <label> <test> <seeded 0|1> <counts...>
  local label=$1 test=$2 seeded=$3; shift 3
  for n in $(printf '%s\n' "$@" | sort -n); do
    local ok=1
    local check=
    [ "$label" = grover ] && [ "$n" -gt 2046 ] && check=answer
    if [ "$seeded" = 1 ]; then
      for s in $SEEDS; do run "$label" "$test" "$n" "$s" "$check" || ok=0; done
    else
      for _ in $(seq "$REPS"); do run "$label" "$test" "$n" || ok=0; done
    fi
    [ $ok -eq 1 ] || { echo "  $label stopped at $n" >&2; return; }
  done
}

want ghz-vec && ladder ghz-vec testGHZAlgo       0 $EVEN $ODD
want bv      && ladder bv      testBVAlgo        1 $EVEN $ODD
want dj      && ladder dj      testDJAlgo        1 $EVEN $ODD
want grover  && ladder grover  testGroversAlgoBig 1 $EVEN
want qft     && ladder qft     testQFT           1 $QFT_N

echo "wrote $OUT"
