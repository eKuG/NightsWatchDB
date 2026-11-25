-- Development helper: builds a query file in the experiment's format from the tuned table (or any
-- table with a `text` column), using row-level document frequencies (the real query file uses piece-level
-- df from the index tool). Candidate terms are tokens of 4 to 20 bytes made only of a-z.
WITH
  cand AS (
    SELECT t, count() AS df FROM (
      SELECT arrayJoin(arrayDistinct(arrayFilter(x -> match(x, '^[a-z]{4,20}$'), tokens(lower(text), 'splitByNonAlpha')))) AS t
      FROM otel.otel_traces_text)
    GROUP BY t),
  n AS (SELECT count() AS rows FROM otel.otel_traces_text),
  rare AS (SELECT groupArray(t) AS a FROM (SELECT t FROM cand ORDER BY abs(toInt64(df) - 2), t LIMIT 10)),
  mid AS (SELECT groupArray(t) AS a FROM (SELECT t FROM cand, n ORDER BY abs(toInt64(df) - toInt64(n.rows * 0.001)), t LIMIT 10)),
  common AS (SELECT groupArray(t) AS a FROM (SELECT t FROM cand, n ORDER BY abs(toInt64(df) - toInt64(n.rows * 0.05)), t LIMIT 10)),
  phr AS (
    SELECT groupArray([x, y]) AS a FROM (
      SELECT DISTINCT x, y FROM (
        SELECT pr.1 AS x, pr.2 AS y
        FROM (SELECT arrayFilter(z -> z != '', tokens(lower(text), 'splitByNonAlpha')) AS tk FROM otel.otel_traces_text LIMIT 300)
        ARRAY JOIN arrayZip(arrayPopBack(tk), arrayPopFront(tk)) AS pr
        WHERE match(x, '^[a-z]{4,20}$') AND match(y, '^[a-z]{4,20}$') AND cityHash64(x, y) % 997 = 0)
      ORDER BY cityHash64(x, y) LIMIT 10)),
  qs AS (
    SELECT arrayConcat(
      arrayMap(i -> map('id', concat('rare-', toString(i - 1)), 'class', 'rare', 'terms', toJSONString([rare.a[i]])), range(1, 11)),
      arrayMap(i -> map('id', concat('mid-', toString(i - 1)), 'class', 'mid', 'terms', toJSONString([mid.a[i]])), range(1, 11)),
      arrayMap(i -> map('id', concat('common-', toString(i - 1)), 'class', 'common', 'terms', toJSONString([common.a[i]])), range(1, 11)),
      arrayMap(i -> map('id', concat('and2-', toString(i - 1)), 'class', 'and2', 'terms', toJSONString([mid.a[i], common.a[i]])), range(1, 11)),
      arrayMap(i -> map('id', concat('and3-', toString(i - 1)), 'class', 'and3', 'terms', toJSONString([mid.a[i], common.a[i], common.a[(i % 10) + 1]])), range(1, 11)),
      arrayMap(i -> map('id', concat('or2-', toString(i - 1)), 'class', 'or2', 'terms', toJSONString([rare.a[i], mid.a[i]])), range(1, 11)),
      arrayMap(i -> map('id', concat('not-', toString(i - 1)), 'class', 'not', 'terms', toJSONString([common.a[i], mid.a[i]])), range(1, 11)),
      arrayMap(i -> map('id', concat('prefix-', toString(i - 1)), 'class', 'prefix', 'prefix', substring(mid.a[i], 1, 4)), range(1, 11)),
      arrayMap(i -> map('id', concat('phrase-', toString(i - 1)), 'class', 'phrase', 'phrase', toJSONString(phr.a[i])), range(1, length(phr.a) + 1))
    ) AS q FROM rare, mid, common, phr)
SELECT concat('{"queries":[', arrayStringConcat(arrayMap(m ->
  concat('{"id":', toJSONString(m['id']), ',"class":', toJSONString(m['class']),
         if(m['class'] = 'prefix', concat(',"prefix":', toJSONString(m['prefix'])),
            if(m['class'] = 'phrase', concat(',"phrase":', m['phrase']), concat(',"terms":', m['terms']))), '}'), q), ','), ']}')
FROM qs
SETTINGS max_block_size = 8, max_threads = 1
