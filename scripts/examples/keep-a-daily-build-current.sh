#!/usr/bin/env bash
# Keeps an extract of the Protomaps daily planet build current, with resumable extracts.
#
# This is an example. A production service, which serves the extract with a web server, runs a
# version of this script. That version also starts and watches the web server.
#
# Usage: keep-a-daily-build-current.sh <directory>
#
# <directory> holds two directories, on one file system:
#   served/   planet.pmtiles, the current extract, and planet.pmtiles.json, its build details
#   cutting/  planet.pmtiles.partial and planet.pmtiles.resume.json, while a cut runs
#
# 1. When served/planet.pmtiles does not exist, the script cuts the newest build into
#    cutting/planet.pmtiles.partial with scripts/resumable-extract.sh. When the cut is complete, the
#    script moves the partial file to served/planet.pmtiles. A web server can serve that file.
# 2. Once a day the script reads the build date in served/planet.pmtiles.json. When the extract is
#    UPDATE_INTERVAL_DAYS old, the script cuts the newest build in cutting/, then moves it over
#    served/planet.pmtiles. A web server that opens the file for each request serves the new file
#    from the next request.
# 3. A cut that stops keeps its partial file and resume state file. The next attempt resumes the same
#    build: after FIRST_CUT_RETRY_SECONDS for the first cut, at the next daily check for an update.
#    When Protomaps has deleted that build, the script cuts the newest build at once.
#
# Settings, from the environment:
#   MAXIMUM_ZOOM           the highest zoom level of the extract (default 14)
#   UPDATE_INTERVAL_DAYS   the age of an extract that makes the script cut a new one (default 30)
#
# Needs: bash, GNU date, curl, jq, pmtiles with --resume-state, and resumable-extract.sh on the PATH.
#
# Known limits:
# - pmtiles extract writes a progress bar to the log. A cut of the planet writes megabytes of
#   progress lines, so a log with a size limit can rotate out the error lines of earlier attempts.
# - The script reads source_url from the resume state file, so it depends on the file format that
#   pmtiles extract --resume-state writes.
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <directory>" >&2
  exit 2
fi

PROTOMAPS_BUILD_URL_PREFIX=https://build.protomaps.com/
SERVED_DIRECTORY=$1/served
CUTTING_DIRECTORY=$1/cutting
SERVED_PLANET_FILE=$SERVED_DIRECTORY/planet.pmtiles
SERVED_BUILD_DETAILS_FILE=$SERVED_PLANET_FILE.json
PARTIAL_FILE=$CUTTING_DIRECTORY/planet.pmtiles.partial
RESUME_STATE_FILE=$CUTTING_DIRECTORY/planet.pmtiles.resume.json
CUT_BUILD_DETAILS_FILE=$CUTTING_DIRECTORY/planet.pmtiles.json
MAXIMUM_ZOOM=${MAXIMUM_ZOOM:-14}
UPDATE_INTERVAL_DAYS=${UPDATE_INTERVAL_DAYS:-30}
BUILD_SEARCH_DAYS=14
UPDATE_CHECK_INTERVAL_SECONDS=86400
FIRST_CUT_RETRY_SECONDS=3600
# The exit status of resumable-extract.sh when the source answers 404.
SOURCE_GONE_STATUS=3

cut_pid=

# Prints the date, as YYYYMMDD, of the newest build that exists, looking back from today. Only a 404
# for a date makes the search look one day further back. Any other answer stops the search, so that a
# build that exists but did not answer does not make the cut take an older build.
newest_build_date() {
  local days_back date status
  for days_back in $(seq 0 "$BUILD_SEARCH_DAYS"); do
    date=$(date -u -d "-$days_back days" +%Y%m%d)
    status=$(curl -sI -o /dev/null -w '%{http_code}' --connect-timeout 30 --max-time 60 \
      "$PROTOMAPS_BUILD_URL_PREFIX$date.pmtiles") || status=000
    case "$status" in
      200)
        echo "$date"
        return 0
        ;;
      404) ;;
      *)
        echo "The check for the build of $date answered $status" >&2
        return 1
        ;;
    esac
  done
  echo "No build at $PROTOMAPS_BUILD_URL_PREFIX in the last $BUILD_SEARCH_DAYS days" >&2
  return 1
}

# Prints the date of the build to cut. A cut in progress continues with its own build, so that it
# resumes instead of starting again.
build_date_to_cut() {
  if [ -f "$RESUME_STATE_FILE" ]; then
    basename "$(jq -r '.source_url' "$RESUME_STATE_FILE")" .pmtiles
    return 0
  fi
  newest_build_date
}

# Cuts a build into the partial file, then moves it and its build details into the served directory.
# Returns SOURCE_GONE_STATUS when the build answered 404, and 1 for any other failure.
cut_build() {
  local build_date status
  build_date=$(build_date_to_cut) || return 1
  echo "Cutting the build of $build_date to zoom $MAXIMUM_ZOOM"
  status=0
  resumable-extract.sh "$PROTOMAPS_BUILD_URL_PREFIX$build_date.pmtiles" "$PARTIAL_FILE" "$RESUME_STATE_FILE" \
    --maxzoom="$MAXIMUM_ZOOM" --keep-resume-state || status=$?
  if [ "$status" -ne 0 ]; then
    echo "The cut of the build of $build_date stopped (exit status $status)" >&2
    return "$status"
  fi
  jq -n --arg build_date "$build_date" --slurpfile extract "$RESUME_STATE_FILE" \
    '{build_date: $build_date, extract: $extract[0]}' >"$CUT_BUILD_DETAILS_FILE"
  # The planet file moves first. A stop between the two moves leaves the new planet file with the old
  # build details, which makes the next check cut again. The other order could leave the old planet
  # file with the new build date, and no update until UPDATE_INTERVAL_DAYS later.
  mv "$PARTIAL_FILE" "$SERVED_PLANET_FILE"
  mv "$CUT_BUILD_DETAILS_FILE" "$SERVED_BUILD_DETAILS_FILE"
  rm -f "$RESUME_STATE_FILE"
  echo "Serving the build of $build_date"
}

# Runs cut_build until it ends with anything but SOURCE_GONE_STATUS. After a 404, resumable-extract.sh
# has deleted the partial file and its resume state, so the next cut_build cuts the newest build.
cut_build_or_newest() {
  local status
  while true; do
    status=0
    cut_build || status=$?
    if [ "$status" -ne "$SOURCE_GONE_STATUS" ]; then
      return "$status"
    fi
    echo "The build is gone; cutting the newest build" >&2
  done
}

# Runs cut_build_or_newest in a process group of its own, and returns its exit status. The process
# group holds the cut, resumable-extract.sh and pmtiles, so a stop signals them all, and nothing else.
run_cut() {
  local status=0
  set -m
  cut_build_or_newest &
  cut_pid=$!
  set +m
  wait "$cut_pid" || status=$?
  cut_pid=
  return "$status"
}

# Succeeds when the served extract is at least UPDATE_INTERVAL_DAYS old, counted from its build date.
update_is_due() {
  local build_date due_epoch
  build_date=$(jq -r '.build_date' "$SERVED_BUILD_DETAILS_FILE" 2>/dev/null) || return 0
  due_epoch=$(date -u -d "$build_date +$UPDATE_INTERVAL_DAYS days" +%s) || return 0
  [ "$(date -u +%s)" -ge "$due_epoch" ]
}

# Stops the cut. The partial file and the resume state file stay, so the next start resumes the cut.
stop_cut() {
  if [ -n "$cut_pid" ]; then
    kill -TERM -- "-$cut_pid" 2>/dev/null || true
    wait "$cut_pid" 2>/dev/null || true
  fi
}

trap 'stop_cut; exit 0' TERM INT

mkdir -p "$SERVED_DIRECTORY" "$CUTTING_DIRECTORY"

if [ ! -f "$SERVED_PLANET_FILE" ] || [ ! -f "$SERVED_BUILD_DETAILS_FILE" ]; then
  echo "No extract in $SERVED_DIRECTORY; cutting one"
  until run_cut; do
    echo "The first cut stopped; trying again in $FIRST_CUT_RETRY_SECONDS seconds" >&2
    sleep "$FIRST_CUT_RETRY_SECONDS" &
    wait $! || true
  done
fi

while true; do
  sleep "$UPDATE_CHECK_INTERVAL_SECONDS" &
  wait $! || true
  if update_is_due; then
    echo "The extract is $UPDATE_INTERVAL_DAYS days old or more; cutting a new one"
    run_cut || echo "The update stopped; trying again at the next check" >&2
  fi
done
