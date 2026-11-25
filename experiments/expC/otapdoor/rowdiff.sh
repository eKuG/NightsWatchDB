#!/bin/sh
# Compares every row the door produced (smoke.sh run with --dump-rows /data/NAME-got.tsv) with the rows an
# independent decode of the generator file (OTLP protobuf, `otapdoor expect`) yields. Prints IDENTICAL or the diff head.
set -eu
NAME=$1
docker run --rm -v expC-otap-data:/data --entrypoint sh expc-otap-door:dev -c "
  otapdoor expect --in /data/$NAME-gen.bin --out /data/$NAME-want.tsv
  sort /data/$NAME-want.tsv > /data/$NAME-want.s; sort /data/$NAME-got.tsv > /data/$NAME-got.s
  wc -l /data/$NAME-want.s /data/$NAME-got.s | head -2
  if cmp -s /data/$NAME-want.s /data/$NAME-got.s; then echo IDENTICAL; sha256sum /data/$NAME-got.s | cut -c1-16; else echo DIFFERENT; diff /data/$NAME-want.s /data/$NAME-got.s | head -c 2000; fi
  rm -f /data/$NAME-*.tsv /data/$NAME-*.s"
