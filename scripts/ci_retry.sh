#!/usr/bin/env bash
# Retry a command until it succeeds or max attempts are exhausted.
#
# Usage:
#   ci_retry.sh [--max N] [--wait-seconds S] -- COMMAND [ARGS...]
#
# Environment (optional overrides):
#   CI_RETRY_MAX_ATTEMPTS      default: 5
#   CI_RETRY_WAIT_SECONDS      default: 3600 (1 hour)
#
# Examples:
#   ci_retry.sh -- python sync_sales_bridge.py
#   CI_RETRY_MAX_ATTEMPTS=3 CI_RETRY_WAIT_SECONDS=1800 -- python weatherman_weather_pulse.py

set -uo pipefail

max_attempts="${CI_RETRY_MAX_ATTEMPTS:-5}"
wait_seconds="${CI_RETRY_WAIT_SECONDS:-3600}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max)
      max_attempts="$2"
      shift 2
      ;;
    --wait-seconds)
      wait_seconds="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 2
      ;;
  esac
done

if [[ $# -lt 1 ]]; then
  echo "Usage: ci_retry.sh [--max N] [--wait-seconds S] -- COMMAND [ARGS...]" >&2
  exit 2
fi

if ! [[ "$max_attempts" =~ ^[1-9][0-9]*$ ]]; then
  echo "Invalid --max value: $max_attempts (must be a positive integer)" >&2
  exit 2
fi

if ! [[ "$wait_seconds" =~ ^[0-9]+$ ]]; then
  echo "Invalid --wait-seconds value: $wait_seconds (must be a non-negative integer)" >&2
  exit 2
fi

echo "ci_retry: up to ${max_attempts} attempt(s), ${wait_seconds}s between failures"

for attempt in $(seq 1 "$max_attempts"); do
  echo "::group::Attempt ${attempt}/${max_attempts}"
  set +e
  "$@"
  exit_code=$?
  set -e
  if [ "$exit_code" -eq 0 ]; then
    echo "ci_retry: succeeded on attempt ${attempt}/${max_attempts}"
    echo "::endgroup::"
    exit 0
  fi
  if [ "$exit_code" -eq 2 ]; then
    echo "ci_retry: permanent failure (exit 2); not retrying."
    echo "::endgroup::"
    exit 2
  fi
  echo "ci_retry: attempt ${attempt}/${max_attempts} failed (exit ${exit_code})"
  echo "::endgroup::"

  if [[ "$attempt" -lt "$max_attempts" ]]; then
    echo "ci_retry: waiting ${wait_seconds}s before next attempt..."
    sleep "$wait_seconds"
  fi
done

echo "ci_retry: all ${max_attempts} attempt(s) failed"
exit 1
