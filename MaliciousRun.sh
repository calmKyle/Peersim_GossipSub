#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Settings (edit if needed)
# -----------------------------
# Root folder containing the SeedCompletionMalicious0_* directories.
# Default: current directory (.)
CONFIG_ROOT="${1:-.}"

# Java options & classpath jars
JAVA_OPTS="-Xmx128000m -Xms2000m"

echo "==> Building classpath from lib/ ..."
LIB_JARS=$(find -L lib/ -type f -name "*.jar" | tr $'\n' :)
CLASSPATH="${LIB_JARS}:classes"

# -----------------------------
# Compile
# -----------------------------
echo "==> Compiling Java sources..."
mkdir -p classes
javac -sourcepath src -classpath "$CLASSPATH" -d classes $(find -L src/ -type f -name "*.java")

echo "==> Compile OK."

# -----------------------------
# Collect all .cfg files
# Layout expected:
#   SeedCompletionMalicious0_[0-4]/TOPIC_AMOUNT_*/K*/Shard_*/MaliciousGossipConfig_seed_*.cfg
# -----------------------------
echo "==> Scanning config files under: ${CONFIG_ROOT}"

# Build list deterministically:
# 1) by SeedCompletionMalicious0_X
# 2) by TOPIC_AMOUNT_*
# 3) by K*
# 4) by Shard_*
# 5) by file name
mapfile -t CFGS < <(
  find "${CONFIG_ROOT}" \
    -type f -name "MaliciousGossipConfig_seed_*.cfg" \
    -path "*/SeedCompletionMalicious0_[0-4]/TOPIC_AMOUNT_*/*/Shard_*/*" \
    | LC_ALL=C sort
)

TOTAL=${#CFGS[@]}
if (( TOTAL == 0 )); then
  echo "No config files found. Check CONFIG_ROOT or directory pattern."
  exit 1
fi

echo "==> Found ${TOTAL} config(s). (Expecting 1600 if you have 5 big folders × 320 each)"

# -----------------------------
# Run simulations sequentially
# -----------------------------
echo "==> Starting simulations..."
idx=0
for cfg_path in "${CFGS[@]}"; do
  ((idx++))
  cfg_file=$(basename "$cfg_path")

  echo "--------------------------------------------------------------------------------"
  echo "[$idx/$TOTAL] Running: $cfg_file"
  echo "Path: $cfg_path"
  start_time=$(date +%s)

  java $JAVA_OPTS -cp "$CLASSPATH" peersim.Simulator "$cfg_path"

  end_time=$(date +%s)
  echo "Finished $cfg_file in $((end_time - start_time))s  [$idx/$TOTAL]"
done

echo "==> All ${TOTAL} simulations complete."
