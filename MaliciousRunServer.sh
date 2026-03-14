#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Tunables
# -----------------------------
CONFIG_ROOT="${1:-.}"       # Root containing SeedCompletionMalicious0_*/
CONCURRENCY="${CONCURRENCY:-5}"
XMX_MB="${XMX_MB:-20000}"   # per-JVM heap cap in MB (e.g. 32000)

# -----------------------------
# Classpath & compile
# -----------------------------
echo "==> Building classpath..."
LIB_JARS=$(find -L lib/ -type f -name "*.jar" | tr $'\n' :)
CLASSPATH="${LIB_JARS}:classes"

echo "==> Compiling..."
mkdir -p classes
javac -sourcepath src -classpath "$CLASSPATH" -d classes $(find -L src/ -type f -name "*.java")
echo "==> Compile OK."

# -----------------------------
# Collect configs (deterministic order)
# -----------------------------
echo "==> Scanning configs under: ${CONFIG_ROOT}"
mapfile -t CFGS < <(
  find "${CONFIG_ROOT}" \
    -type f -name "MaliciousGossipConfig_seed_*.cfg" \
    -path "*/SeedCompletionMalicious0_[0-5]/TOPIC_AMOUNT_*/*/Shard_*/*" \
    | LC_ALL=C sort
)

TOTAL=${#CFGS[@]}
(( TOTAL > 0 )) || { echo "No configs found."; exit 1; }
echo "==> Found ${TOTAL} config(s)."

# -----------------------------
# Run N at a time
# -----------------------------
export CLASSPATH
export XMX_MB

run_one() {
  cfg_path="$1"
  cfg_file="$(basename "$cfg_path")"
  start=$(date +%s)
  echo "[START $(date -Iseconds)] $cfg_file"
  # You can tweak GC flags if you like:
  java -Xmx${XMX_MB}m -Xms2048m -XX:+UseG1GC -cp "$CLASSPATH" peersim.Simulator "$cfg_path"
  rc=$?
  dur=$(( $(date +%s) - start ))
  echo "[DONE  $(date -Iseconds) rc=${rc} dur=${dur}s] $cfg_file"
  exit $rc
}

export -f run_one

echo "==> Launching with concurrency=${CONCURRENCY}, per-JVM heap ~${XMX_MB} MB"
# Feed null-delimited args safely, run 4 at a time
printf '%s\0' "${CFGS[@]}" | xargs -0 -n1 -P "${CONCURRENCY}" bash -c 'run_one "$0"' 

echo "==> All ${TOTAL} simulations finished."

