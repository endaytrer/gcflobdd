#!/usr/bin/env bash
# Run the network-verification kernels across every backend and every size on
# one ladder, appending a CSV row per run.
#
#   BACKENDS="jdd ndd gcflobdd-abs" KS="4 6 8" scripts/compare_netverify.sh
#
# Knobs (all optional):
#   OUT        output CSV                     (results/netverify.csv)
#   BACKENDS   which engines                  (all five)
#   KS         fat-tree sizes                 (4 6 8 10)
#   ROUTES     extra injected routes, per k   (scaled: 25*k)
#   ACL_RULES  ACL rules per access list      (8)
#   SEEDS      generator seeds                (1)
#   WORKLOAD   acl | ap | reach | equiv | all (all)
#   CAP        give up past this many atoms   (0 = uncapped)
#   TIMEOUT    seconds per run                (600)
#   APPEND     1 to keep an existing CSV
#   JAVA_OPTS  extra JVM flags, e.g. -Xmx16g
set -uo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
BENCH="$HERE/bench/netverify"
OUT="${OUT:-$HERE/results/netverify.csv}"
BACKENDS="${BACKENDS:-jdd ndd gcflobdd-fg gcflobdd-ab gcflobdd-abs}"
KS="${KS:-4 6 8 10}"
ACL_RULES="${ACL_RULES:-8}"
SEEDS="${SEEDS:-1}"
WORKLOAD="${WORKLOAD:-all}"
CAP="${CAP:-0}"
TIMEOUT="${TIMEOUT:-600}"
APPEND="${APPEND:-0}"
export JAVA_OPTS="${JAVA_OPTS:--Xmx12g}"

[ -f "$BENCH/env.sh" ] || { echo "run bench/netverify/build.sh first" >&2; exit 1; }
# shellcheck disable=SC1091
source "$BENCH/env.sh"

mkdir -p "$(dirname "$OUT")"
if [ "$APPEND" != 1 ] || [ ! -s "$OUT" ]; then
  echo "impl,workload,k,routes,acl_rules,seed,dataset,fib_rules,acl_rule_count,predicates,wall_s,peak_rss_kb,fwd_ms,acl_ms,ap_ms,reach_ms,total_ms,atoms,pairs,equal_acl_pairs,fwd_fraction,acl_fraction,engine_nodes,label_nodes,engine_bytes,max_nodes,max_edges,max_conv,status" > "$OUT"
fi

tmp_out=$(mktemp); tmp_time=$(mktemp)
trap 'rm -f "$tmp_out" "$tmp_time"' EXIT

# GNU `time` takes -f and reports peak RSS in kB; BSD's takes -l and reports
# bytes. `timeout` is coreutils, absent on a stock macOS.
if /usr/bin/time -f '%e %M' -o /dev/null true >/dev/null 2>&1; then
  TIME_ARGS=(-f '%e %M'); TIME_STYLE=gnu
else
  TIME_ARGS=(-l); TIME_STYLE=bsd
fi
TIMEOUT_BIN=$(command -v timeout || command -v gtimeout || true)

# Stand-in for coreutils `timeout`, same 124-on-expiry contract. `set -m` puts
# the child in its own process group so the kill reaches the JVM itself and not
# just the wrapper script, which would otherwise leave a JVM burning a core
# through every run that follows.
sh_timeout() {
  local secs=$1; shift
  local tmp fired; tmp=$(mktemp -t shto); fired=$tmp.fired; rm -f "$fired"
  set -m
  "$@" & local pid=$!
  (
    sleep "$secs"
    : > "$fired"
    kill -TERM -- -"$pid" 2>/dev/null
    sleep 2
    kill -KILL -- -"$pid" 2>/dev/null
  ) 2>/dev/null & local watch=$!
  local rc=0
  wait "$pid" 2>/dev/null || rc=$?
  [ -e "$fired" ] && rc=124
  kill -KILL -- -"$watch" 2>/dev/null
  wait "$watch" 2>/dev/null
  kill -KILL -- -"$pid" 2>/dev/null
  rm -f "$tmp" "$fired"
  set +m
  return $rc
}

get() { sed -n "s/.* $1=\([^ ]*\).*/\1/p" "$tmp_out" | head -1; }

# run <backend> <k> <routes> <aclrules> <seed> <datadir>
run_one() {
  local impl=$1 k=$2 routes=$3 acl=$4 seed=$5 dir=$6
  local rc=0
  if [ -n "$TIMEOUT_BIN" ]; then
    /usr/bin/time "${TIME_ARGS[@]}" -o "$tmp_time" \
      "$TIMEOUT_BIN" "$TIMEOUT" "$BENCH/run.sh" \
        --backend "$impl" --data "$dir" --workload "$WORKLOAD" --cap "$CAP" \
      > "$tmp_out" 2>/dev/null
    rc=$?
  else
    sh_timeout "$TIMEOUT" \
      /usr/bin/time "${TIME_ARGS[@]}" -o "$tmp_time" "$BENCH/run.sh" \
        --backend "$impl" --data "$dir" --workload "$WORKLOAD" --cap "$CAP" \
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

  local label; label=$(get impl); [ -n "$label" ] || label=$impl
  local inner; inner=$(get status)
  [ "$status" = ok ] && [ -n "$inner" ] && status=$inner

  echo "$label,$WORKLOAD,$k,$routes,$acl,$seed,$(basename "$dir"),$(get fib_rules),$(get acl_rules),$(get predicates),$wall,$rss,$(get fwd_ms),$(get acl_ms),$(get ap_ms),$(get reach_ms),$(get total_ms),$(get atoms),$(get pairs),$(get equal_acl_pairs),$(get fwd_fraction),$(get acl_fraction),$(get engine_nodes),$(get label_nodes),$(get engine_bytes),$(get max_nodes),$(get max_edges),$(get max_conv),$status" >> "$OUT"

  printf '%-32s k=%-3s seed=%-2s %-8s atoms=%-7s pairs=%-7s %sms (wall %ss)\n' \
    "$label" "$k" "$seed" "$status" "$(get atoms)" "$(get pairs)" \
    "$(get total_ms)" "${wall:-?}"

  [ "$status" = ok ]
}

for seed in $SEEDS; do
  for k in $KS; do
    routes="${ROUTES:-$(( 25 * k ))}"
    dir="$BENCH/data/ft${k}_r${routes}_a${ACL_RULES}_s${seed}"
    if [ ! -f "$dir/fib.txt" ]; then
      java -cp "$NETBENCH_CP" netbench.gen.Fattree \
        --k "$k" --routes "$routes" --acl-rules "$ACL_RULES" --seed "$seed" --out "$dir"
    fi
    for impl in $BACKENDS; do
      case " ${STOPPED:-} " in *" $impl "*) continue;; esac
      run_one "$impl" "$k" "$routes" "$ACL_RULES" "$seed" "$dir" \
        || STOPPED="${STOPPED:-} $impl"   # a ladder stops at the first failure
    done
  done
done

echo
echo "-> $OUT"
