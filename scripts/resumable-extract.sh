#!/usr/bin/env bash
# Runs `pmtiles extract --resume-state` until the extract succeeds, with a pause between runs.
#
# Usage: resumable-extract.sh <source-url> <output> <resume-state-file> [extract options...]
#
# The script passes --resume-state and --restart-on-unresumable to every run. Each run therefore
# continues from the progress of the run before it. A source that changed starts the extract again.
#
# Pause between two runs:
# - A run that added bytes to the output: 1 minute.
# - A run that added no bytes: 1, 2, 4, 8, 16 and 32 minutes, then 1 hour.
# The script stops after 10 runs in a row that add no bytes.
#
# Exit status:
#   0  The extract is complete.
#   1  The extract did not start, because the source could not be read, or 10 runs in a row added
#      no bytes. The output and the resume state file stay, so a later call resumes.
#   3  The source answers 404. The script deletes the output and the resume state file.
#
# Needs: pmtiles (or the path in $PMTILES), curl and jq.
set -euo pipefail

if [ $# -lt 3 ]; then
  echo "Usage: $0 <source-url> <output> <resume-state-file> [extract options...]" >&2
  exit 2
fi

source_url=$1
output=$2
resume_state=$3
shift 3
pmtiles=${PMTILES:-pmtiles}

maximum_runs_without_progress=10
progress_pause_seconds=60
no_progress_pause_seconds=(60 120 240 480 960 1920)
longest_pause_seconds=3600

# Prints the number of source bytes that the resume state file records as on disk, or 0.
bytes_done() {
  if [ -f "$resume_state" ]; then
    jq '[.range_bytes_done // {} | .[]] | add // 0' "$resume_state"
  else
    echo 0
  fi
}

# Succeeds when the source answers 404.
source_is_gone() {
  local status
  status=$(curl -s -o /dev/null -w '%{http_code}' -r 0-0 "$source_url") || return 1
  [ "$status" = 404 ]
}

runs_without_progress=0
while true; do
  if source_is_gone; then
    echo "The source $source_url answers 404. Deleting $output and $resume_state." >&2
    rm -f "$output" "$resume_state"
    exit 3
  fi

  before=$(bytes_done)
  status=0
  "$pmtiles" extract "$source_url" "$output" --resume-state="$resume_state" --restart-on-unresumable "$@" || status=$?
  if [ "$status" -eq 0 ]; then
    exit 0
  fi

  if [ ! -f "$resume_state" ]; then
    echo "The extract did not start (exit status $status). The source could not be read." >&2
    exit 1
  fi

  after=$(bytes_done)
  if [ "$after" -gt "$before" ]; then
    runs_without_progress=0
    pause=$progress_pause_seconds
    echo "The extract stopped (exit status $status) after $((after - before)) more bytes. Resuming in $pause seconds." >&2
  else
    runs_without_progress=$((runs_without_progress + 1))
    if [ "$runs_without_progress" -ge "$maximum_runs_without_progress" ]; then
      echo "$runs_without_progress runs in a row added no bytes (last exit status $status). Stopping." >&2
      exit 1
    fi
    index=$((runs_without_progress - 1))
    if [ "$index" -lt "${#no_progress_pause_seconds[@]}" ]; then
      pause=${no_progress_pause_seconds[$index]}
    else
      pause=$longest_pause_seconds
    fi
    echo "The extract stopped (exit status $status) and added no bytes, $runs_without_progress runs in a row. Resuming in $pause seconds." >&2
  fi
  sleep "$pause"
done
