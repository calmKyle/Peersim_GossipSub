#!/usr/bin/env bash
set -Eeuo pipefail

# Usage:
#   ./MaliciousRun.sh [PROJECT_ROOT=.] [LOG_ROOT=run_logs] [--no-compile]
CONFIG_ROOT="${1:-.}"
LOG_ROOT="${2:-run_logs}"
NO_COMPILE="${3:-}"

TIMEOUT_SECS="${TIMEOUT_SECS:-0}"
JAVA_OPTS="${JAVA_OPTS:--Xmx128000m -Xms2000m -Djava.security.egd=file:/dev/./urandom}"

# Optional helpers (graceful if missing)
STDBUF="$(command -v stdbuf >/dev/null 2>&1 && echo "stdbuf -oL -eL" || true)"
TIMEOUT_CMD=""
if (( TIMEOUT_SECS > 0 )) && command -v timeout >/dev/null 2>&1; then
  TIMEOUT_CMD="timeout --preserve-status ${TIMEOUT_SECS}"
fi

echo "==> Building classpath from lib/ ..."
LIB_JARS=$(find -L lib/ -type f -name "*.jar" 2>/dev/null | tr $'\n' :)
CLASSPATH="${LIB_JARS}:classes"
mkdir -p classes "$LOG_ROOT"

# -------- compile once / when changed --------
need_compile=true
if [[ "$NO_COMPILE" == "--no-compile" ]]; then
  need_compile=false
elif [[ -f classes/.src.sha256 ]] && command -v sha256sum >/dev/null 2>&1; then
  cur=$(find -L src -type f -name "*.java" -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}')
  prev=$(cut -d' ' -f1 classes/.src.sha256 || true)
  [[ "$cur" == "$prev" ]] && need_compile=false
elif [[ ! $(find -L src -type f -name "*.java" -newer classes 2>/dev/null | head -n1) ]]; then
  need_compile=false
fi

if $need_compile; then
  echo "==> Compiling Java sources (changes detected or first run)..."
  # shellcheck disable=SC2046
  javac -sourcepath src -classpath "$CLASSPATH" -d classes $(find -L src -type f -name "*.java" -print)
  echo "==> Compile OK."
  if command -v sha256sum >/dev/null 2>&1; then
    find -L src -type f -name "*.java" -print0 | sort -z | xargs -0 sha256sum | sha256sum > classes/.src.sha256
  fi
else
  echo "==> Skipping compile (no changes)."
fi

echo "==> Scanning + running configs sequentially..."
idx=0
fail=0

# IMPORTANT: Use a plain line-delimited list (safer for shells, simple to read).
# Your paths don't contain newlines, so this is fine and very reliable.
find "${CONFIG_ROOT}" \
  -type f -name "MaliciousGossipConfig_seed_*.cfg" \
  -path "*/SeedCompletionMalicious0_*/TOPIC_AMOUNT_*/*/Shard_*/*" \
  | LC_ALL=C sort \
  | while IFS= read -r cfg_path; do
      idx=$((idx+1))
      cfg_file=$(basename "$cfg_path")
      rel="${cfg_path#${CONFIG_ROOT%/}/}"
      log_dir="${LOG_ROOT}/$(dirname "$rel")"
      mkdir -p "$log_dir"
      log_file="${log_dir}/${cfg_file%.cfg}.log"

      echo "--------------------------------------------------------------------------------"
      echo "[$idx] Running: $cfg_file"
      echo "Path: $cfg_path"
      echo "Log : $log_file"
      start_time=$(date +%s)

      set +e
      if [[ -n "$TIMEOUT_CMD" ]]; then
        $STDBUF $TIMEOUT_CMD java $JAVA_OPTS -cp "$CLASSPATH" peersim.Simulator "$cfg_path" >>"$log_file" 2>&1
        rc=$?
      else
        $STDBUF java $JAVA_OPTS -cp "$CLASSPATH" peersim.Simulator "$cfg_path" >>"$log_file" 2>&1
        rc=$?
      fi
      set -e

      end_time=$(date +%s)
      dur=$((end_time - start_time))

      if [[ $rc -eq 0 ]]; then
        echo "✅ Finished $cfg_file in ${dur}s"
      elif [[ $rc -eq 124 ]]; then
        echo "⏱️  TIMEOUT after ${TIMEOUT_SECS}s — see $log_file"
        fail=$((fail+1))
      else
        echo "❌ Exit code $rc — see $log_file"
        fail=$((fail+1))
      fi
    done

echo "==> Done. Total run attempts: ${idx}. Failures: ${fail}"
echo "Logs in: ${LOG_ROOT}"
