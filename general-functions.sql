-- General SQL functions

CREATE OR REPLACE FUNCTION get_ints_from_text(TEXT) RETURNS int[] AS $$
  select array_remove(regexp_split_to_array($1,'[^0-9]+','i'),'')::int[];
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION _final_median(anyarray) RETURNS float8 AS $$
  WITH q AS
  (
     SELECT val
     FROM unnest($1) val
     WHERE VAL IS NOT NULL
     ORDER BY 1
  ),
  cnt AS
  (
    SELECT COUNT(*) AS c FROM q
  )
  SELECT AVG(val)::float8
  FROM
  (
    SELECT val FROM q
    LIMIT  2 - MOD((SELECT c FROM cnt), 2)
    OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)
  ) q2;
$$ LANGUAGE SQL IMMUTABLE;

-- something wrong with array functions?
CREATE AGGREGATE median(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=_final_median,
  INITCOND='{}'
);

CREATE OR REPLACE FUNCTION array_reverse(anyarray) RETURNS anyarray AS $$
SELECT ARRAY(
    SELECT $1[i]
    FROM generate_subscripts($1,1) AS s(i)
    ORDER BY i DESC
);
$$ LANGUAGE 'sql' STRICT IMMUTABLE;


-- weighted median function
-- based on code found here: https://forums.postgresql.fr/viewtopic.php?id=4529
-- and many other fixes...
-- input parameters:
--  data table (string) - table name with data values
--  x (string) - x column name, from which to calculate the median
--  w (string) - weights column name
CREATE OR REPLACE FUNCTION public.weighted_median(
    vals regclass,
    x VARCHAR,
    w VARCHAR,
    OUT median DOUBLE PRECISION)
RETURNS float8 AS $$
DECLARE
    table_size BIGINT;
BEGIN
    EXECUTE format ('SELECT count(*) FROM %s',vals) INTO table_size;
    EXECUTE format(
        'WITH weighted_median AS (SELECT %I AS x, '||
        'last_value(%I) OVER (ORDER BY %I ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prevx, '||
        'SUM(coalesce(%I,0)) OVER (ORDER BY %I ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS runsum, '||
        'SUM(coalesce(%I,0)) OVER (ORDER BY %I ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prevsum, '||
        'SUM(coalesce(%I,0)) OVER ()  AS total_weight FROM  %s) '||
        'SELECT CASE WHEN %L %% 2 = 0 THEN (avg(x)+avg(prevx))/2.0 ELSE avg(x) END '||
        'FROM weighted_median ' ||
        'WHERE total_weight / 2 BETWEEN prevsum AND runsum'
        ,x,x,x,w,x,w,x,w,vals,table_size
        ) INTO median;
END
$$
LANGUAGE plpgsql;


-- weighted median function with grouping
-- input parameters:
--  data table (string) - table name with data values
--  x (string) - x column name, from which to calculate the median
--  w (string) - weights column name
--  g (string) - grouping column name
CREATE OR REPLACE FUNCTION public.weighted_median_by_group(
    vals regclass,
    x VARCHAR,
    w VARCHAR,
    g VARCHAR)
    RETURNS TABLE(group_id TEXT, weighted_median NUMERIC)
    AS $$
BEGIN
    RETURN QUERY EXECUTE format(
        'WITH weighted_median AS (SELECT %I AS g, %I AS x, '||
        'last_value(%I) OVER (PARTITION BY %I ORDER BY %I ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prevx, '||
        'SUM(coalesce(%I,0)) OVER (PARTITION BY %I ORDER BY %I ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS runsum, '||
        'SUM(coalesce(%I,0)) OVER (PARTITION BY %I ORDER BY %I ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prevsum, '||
        'SUM(coalesce(%I,0)) OVER (PARTITION BY %I) AS total_weight FROM  %s) '||
        'SELECT a.g::TEXT, CASE WHEN max(b.count) %% 2 = 0 THEN ((max(a.x)+max(a.prevx))/2.0)::numeric ELSE max(a.x)::numeric END '||
        'FROM weighted_median AS a, (SELECT %I, count(*) count FROM %s GROUP BY %I) AS b ' ||
        'WHERE (total_weight / 2 BETWEEN prevsum AND runsum) AND a.g = b.%I '||
        'GROUP BY a.g'
        ,g,x,x,g,x,w,g,x,w,g,x,w,g,vals,g,vals,g,g
        );
END
$$
LANGUAGE plpgsql;


--
-- Testing the median functions
DROP TABLE IF EXISTS tags_summary.temp_table;
CREATE TABLE tags_summary.temp_table AS
WITH  vals (k,v) AS (
    VALUES (0,325), (1, -100), (5,50), (3,NULL), (2.7,1153), (2,98)
    )
SELECT * FROM vals;

DROP TABLE IF EXISTS tags_summary.temp_table2;
CREATE TABLE tags_summary.temp_table2 AS
WITH  vals (k,v) AS (
    VALUES (0,0), (1, 0), (5,0), (3,0), (2.7,0), (2,0)
    )
SELECT * FROM vals;

SELECT median(k) FROM tags_summary.temp_table;
SELECT median(k) FROM tags_summary.temp_table2;

SELECT weighted_median('tags_summary.temp_table','k','v');
SELECT weighted_median('tags_summary.temp_table2','k','v');
