#!/bin/sh
# usage: cold.sh DATASET
# Stops the ClickHouse container, flushes and drops the Linux page cache of the Docker VM (dentries and
# inodes too) from a privileged container, then starts ClickHouse again (empty mark and uncompressed
# caches) and waits until it answers. Prints the VM's Cached figure (kB) before and after.
set -eu
DS=${1:?usage: cold.sh DATASET}
. "$(dirname "$0")/common.sh"
docker inspect "$CH" >/dev/null 2>&1 || die "no container $CH (run load.sh $DS first)"
docker stop -t 60 "$CH" >/dev/null
out=$(docker run --rm --privileged --pid=host "$HELPER_IMAGE" sh -c '
  b=$(awk "/^Cached:/{print \$2}" /proc/meminfo); sync; echo 3 > /proc/sys/vm/drop_caches
  a=$(awk "/^Cached:/{print \$2}" /proc/meminfo); echo "$b $a"')
set -- $out
docker start "$CH" >/dev/null
wait_ch
printf '{"task":"cold","dataset":"%s","vm_cached_kb_before":%s,"vm_cached_kb_after":%s}\n' "$DS" "$1" "$2"
