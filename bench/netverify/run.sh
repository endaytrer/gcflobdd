#!/usr/bin/env bash
# Run one backend on one dataset. Thin wrapper that supplies the classpath, the
# native library path, and the JDK 24 native-access flag.
#
#   ./run.sh --backend ndd --data data/fattree4_r0_a8_s1 --workload all
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
[ -f "$HERE/env.sh" ] || { echo "run build.sh first" >&2; exit 1; }
# shellcheck disable=SC1091
source "$HERE/env.sh"
exec java ${JAVA_OPTS:-} \
  --enable-native-access=ALL-UNNAMED \
  -Djava.library.path="$NETBENCH_LIB" \
  -cp "$NETBENCH_CP" \
  netbench.Main "$@"
