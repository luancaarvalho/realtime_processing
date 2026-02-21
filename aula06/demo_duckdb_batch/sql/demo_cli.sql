-- Executar com:
-- duckdb data/small_data.duckdb -f sql/demo_cli.sql

SHOW TABLES;

DESCRIBE bronze_sales;

SELECT *
FROM bronze_sales
ORDER BY event_time
LIMIT 8;

SELECT *
FROM gold_kpis
ORDER BY gmv_total DESC;

SELECT channel, SUM(gmv) AS gmv_total
FROM bronze_sales
GROUP BY 1
ORDER BY gmv_total DESC;
