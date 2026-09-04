#!/usr/bin/env bash
#
# The control group: CUDD's ADDs, from the reference repository's own
# `examples/cudd_test` driver.
#
# Five algorithms, a cap per size, and no larger size attempted once one fails
# -- these blow up fast enough that climbing past a failure only wastes the cap.
# The cap is 120 s, the same order as the one the two CFLOBDDs ran under, so a
# timeout here is a statement about the representation rather than about the
# control group having been given less room.
#
# The driver does not report uniformly.  GHZ, BV, DJ and Grover end with
# `nodeCount: <n> time_taken: <s>`; the Fourier path instead prints the real and
# imaginary ADDs' node counts on a bare line followed by `time taken: <s>`
# (space, not underscore), because it carries the state as a *pair* of ADDs.
# Both shapes are parsed here, and the Fourier row records the sum, which is
# what the driver's own return value is.
#
# Usage:  CUDD=<cudd_test> OUT=results/macos-m1pro/cudd.csv results/macos-m1pro/cudd.sh
set -uo pipefail

here=$(cd "$(dirname "$0")/../.." && pwd)
CUDD=${CUDD:-$here/../cflobdd/examples/cudd_test}
OUT=${OUT:-$here/results/cudd.csv}
LOGS=${LOGS:-}
TIMEOUT=${TIMEOUT:-120}
PMAX=${PMAX:-12}

[ -x "$CUDD" ] || { echo "error: $CUDD not built" >&2; exit 1; }
mkdir -p "$(dirname "$OUT")"
[ -n "$LOGS" ] && mkdir -p "$LOGS"

echo "algo,p,qubits,nodes,time_s,wall_s,peak_rss_kb,status" > "$OUT"

tmp_out=$(mktemp); tmp_time=$(mktemp)
trap 'rm -f "$tmp_out" "$tmp_time"' EXIT

if /usr/bin/time -f '%e %M' -o /dev/null true >/dev/null 2>&1; then
  TIME_ARGS=(-f '%e %M'); TIME_STYLE=gnu
else
  TIME_ARGS=(-l); TIME_STYLE=bsd
fi
TIMEOUT_BIN=$(command -v timeout || command -v gtimeout || true)

# As in scripts/compare_cflobdd.sh: `set -m` gives the child its own process
# group so the kill reaches the measured binary and not just `time` around it.
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

for algo in GHZ BV DJ grover fourier; do
  for p in $(seq 1 "$PMAX"); do
    q=$((2 ** p))

    if [ -n "$TIMEOUT_BIN" ]; then
      /usr/bin/time "${TIME_ARGS[@]}" -o "$tmp_time" \
        "$TIMEOUT_BIN" "$TIMEOUT" "$CUDD" "$algo" "$p" > "$tmp_out" 2>/dev/null
      rc=$?
    else
      sh_timeout "$TIMEOUT" \
        /usr/bin/time "${TIME_ARGS[@]}" -o "$tmp_time" "$CUDD" "$algo" "$p" \
        > "$tmp_out" 2>/dev/null
      rc=$?
    fi

    st=ok
    [ $rc -eq 124 ] && st=timeout
    [ $rc -ne 0 ] && [ $rc -ne 124 ] && st="exit$rc"
    [ -n "$LOGS" ] && cp "$tmp_out" "$LOGS/$algo-$p.txt"

    if [ "$TIME_STYLE" = gnu ]; then
      read -r wall rss < <(tail -n 1 "$tmp_time" 2>/dev/null) || { wall=; rss=; }
    else
      wall=$(awk '/ real /{print $1; exit}' "$tmp_time" 2>/dev/null)
      rss=$(awk '/maximum resident set size/{printf "%d", $1/1024; exit}' "$tmp_time" 2>/dev/null)
    fi
    case "$wall" in ''|*[!0-9.]*) wall=; rss=;; esac

    nodes=; t=
    if [ "$st" = ok ]; then
      nodes=$(grep -o "nodeCount: [0-9]*" "$tmp_out" | tail -1 | awk '{print $2}')
      t=$(grep -o "time_taken: [0-9.e+-]*" "$tmp_out" | tail -1 | awk '{print $2}')
      # The Fourier path's own shape: "<real> <imag>" on the line immediately
      # before "time taken: <s>".  Anchoring on that line matters -- Deutsch-Jozsa
      # prints "<index> <bit>" for every input, which a bare two-number pattern
      # would happily match instead.
      if [ "$algo" = fourier ]; then
        nodes=$(grep -B1 "^time taken: " "$tmp_out" | head -1 |
                awk 'NF == 2 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ {print $1 + $2}')
        t=$(grep -o "time taken: [0-9.e+-]*" "$tmp_out" | tail -1 | awk '{print $3}')
      fi
    fi

    echo "$algo,$p,$q,$nodes,$t,$wall,$rss,$st" | tee -a "$OUT"
    [ "$st" != ok ] && break
  done
done
