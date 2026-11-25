#!/bin/sh
# usage: clean.sh DATASET [images]
# Removes the containers, network and volume this stream created for DATASET and its result files.
# "images" also removes the tracegen image built here and the pulled ClickHouse and collector images.
set -eu
DS=${1:?usage: clean.sh DATASET [images]}
. "$(dirname "$0")/common.sh"
for c in "$CH" "$OT" "$P-tracegen"; do docker rm -f "$c" >/dev/null 2>&1 || true; done
docker network rm "$NET" >/dev/null 2>&1 || true
docker volume rm "$VOL" >/dev/null 2>&1 || true
rm -f "$RESULTS"/*-"$DS".*
if [ "${2:-}" = images ]; then
  docker rmi "$GEN_IMAGE" "$CH_IMAGE" "$OTEL_IMAGE" >/dev/null 2>&1 || true
fi
echo "cleaned $DS"
