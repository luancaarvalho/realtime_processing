SET 'auto.offset.reset' = 'earliest';

DROP TABLE IF EXISTS sales_gmv_g5;

CREATE TABLE sales_gmv_g5 AS
  SELECT
    symbol,
    WINDOWSTART AS window_start_ms,
    WINDOWEND AS window_end_ms,
    COUNT(*) AS cnt,
    SUM(price * qty) AS gmv
  FROM sales_raw
  WINDOW TUMBLING (SIZE 10 SECONDS, GRACE PERIOD 5 SECONDS)
  GROUP BY symbol
  EMIT CHANGES;

SELECT * FROM sales_gmv_g5 EMIT CHANGES;
