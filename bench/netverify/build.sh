#!/usr/bin/env bash
# Build the network-verification harness: the JNI cdylib, NDD's core compiled
# from source, and this directory's Java.
#
#   NDD_REPO=/path/to/NDD ./build.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
NDD_REPO="${NDD_REPO:-/Users/endaytrer/src/NDD}"
NDD_JAR="$NDD_REPO/target/ndd-1.0.1-jar-with-dependencies.jar"

[ -d "$NDD_REPO" ] || { echo "NDD repo not found at $NDD_REPO" >&2; exit 1; }
[ -f "$NDD_JAR" ]  || { echo "NDD fat jar not found at $NDD_JAR" >&2; exit 1; }

echo "==> gcflobdd-jni (cdylib)"
( cd "$ROOT/gcflobdd-jni" && cargo build --release )

# NDD's own pom excludes AtomizedNDD/AtomizedNodeTable -- they still use the old
# object-based NDD API and no longer type-check. We do not need that AP layer.
echo "==> NDD core from source (excluding AtomizedNDD, as NDD's pom does)"
NDD_CLASSES="$HERE/lib/ndd-classes"
rm -rf "$NDD_CLASSES"; mkdir -p "$NDD_CLASSES"
find "$NDD_REPO/src/main/java/org/ants/jndd" "$NDD_REPO/src/main/java/jdd" -name '*.java' \
  | grep -v -e AtomizedNDD.java -e AtomizedNodeTable.java > "$HERE/lib/ndd-srcs.txt"
javac -nowarn -d "$NDD_CLASSES" -cp "$NDD_JAR" "@$HERE/lib/ndd-srcs.txt"

echo "==> netbench"
CLASSES="$HERE/classes"
rm -rf "$CLASSES"; mkdir -p "$CLASSES"
find "$HERE/src" -name '*.java' > "$HERE/lib/srcs.txt"
javac -nowarn -d "$CLASSES" -cp "$NDD_CLASSES:$NDD_JAR" "@$HERE/lib/srcs.txt"

cat > "$HERE/env.sh" <<EOF
# sourced by run.sh and scripts/compare_netverify.sh
NETBENCH_CP="$CLASSES:$NDD_CLASSES:$NDD_JAR"
NETBENCH_LIB="$ROOT/gcflobdd-jni/target/release"
EOF

echo "==> ok"
echo "    classpath: $CLASSES:$NDD_CLASSES:$NDD_JAR"
echo "    native:    $ROOT/gcflobdd-jni/target/release"
