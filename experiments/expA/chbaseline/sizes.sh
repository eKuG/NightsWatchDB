#!/bin/sh
# usage: sizes.sh DATASET
# One JSON line: per table (active parts only) rows, parts, bytes on disk, compressed and uncompressed
# bytes; per column of the traces table the same from system.parts_columns; and the size of the
# ClickHouse store directory in the volume. Reads whatever state the tables are in: load.sh ends with
# OPTIMIZE ... FINAL and waits for system.merges to be empty, so by default these are fully merged parts.
# Set OPTIMIZE=1 to run OPTIMIZE ... FINAL here first (modifies the volume).
set -eu
DS=${1:?usage: sizes.sh DATASET}
. "$(dirname "$0")/common.sh"
start_ch
if [ "${OPTIMIZE:-0}" = 1 ]; then
  chq -q 'OPTIMIZE TABLE otel.otel_traces FINAL'
  chq -q 'OPTIMIZE TABLE otel.otel_traces_trace_id_ts FINAL'
fi
while [ "$(chq -q 'SELECT count() FROM system.merges')" != 0 ]; do sleep 1; done
store=$(docker exec "$CH" du -sb /var/lib/clickhouse/store | cut -f1)
chq --format JSONEachRow --output_format_json_quote_64bit_integers=0 -q "
SELECT '$DS' AS dataset, 'sizes' AS task, $store AS store_dir_bytes,
  (SELECT groupArray(CAST((table, rows, parts, bytes_on_disk, compressed, uncompressed) AS
      Tuple(table String, rows UInt64, parts UInt64, bytes_on_disk UInt64, compressed_bytes UInt64, uncompressed_bytes UInt64)))
   FROM (SELECT table, sum(rows) AS rows, count() AS parts, sum(bytes_on_disk) AS bytes_on_disk,
                sum(data_compressed_bytes) AS compressed, sum(data_uncompressed_bytes) AS uncompressed
         FROM system.parts WHERE database = 'otel' AND active GROUP BY table ORDER BY table)) AS tables,
  (SELECT groupArray(CAST((column, compressed, uncompressed) AS
      Tuple(column String, compressed_bytes UInt64, uncompressed_bytes UInt64)))
   FROM (SELECT column, sum(column_data_compressed_bytes) AS compressed, sum(column_data_uncompressed_bytes) AS uncompressed
         FROM system.parts_columns WHERE database = 'otel' AND table = 'otel_traces' AND active
         GROUP BY column ORDER BY compressed DESC)) AS traces_columns"
