-- ==========================================================
-- SEÇÃO: C-LEVEL
-- ===========================================================
-- GMV Total 
SELECT
  SUM("public"."kpi_clevel"."gmv") AS "sum"
FROM
  "public"."kpi_clevel"
-----------------------------------------------------------
-- Net Profit 
SELECT
  SUM("public"."kpi_clevel"."net_profit") AS "sum"
FROM
  "public"."kpi_clevel"
-----------------------------------------------------------
-- Ticket Médio
SELECT
  AVG("public"."kpi_clevel"."avg_ticket") AS "avg"
FROM
  "public"."kpi_clevel"


-- ==========================================================
-- SEÇÃO: VENDAS
-- ==========================================================
-- Número de Pedidos 
SELECT
  SUM("public"."kpi_clevel"."total_orders") AS "sum"
FROM
  "public"."kpi_clevel"
-----------------------------------------------------------
-- Desconto Médio
SELECT
  AVG("public"."kpi_clevel"."total_discount") AS "avg"
FROM
  "public"."kpi_clevel"
-----------------------------------------------------------
-- Ticket Médio 
SELECT
  AVG("public"."kpi_clevel"."avg_ticket") AS "avg"
FROM
  "public"."kpi_clevel"
-----------------------------------------------------------
-- GMV POR Vendedor
SELECT
  "public"."kpi_seller"."seller_name" AS "seller_name",
  SUM("public"."kpi_seller"."gmv") AS "sum"
FROM
  "public"."kpi_seller"
GROUP BY
  "public"."kpi_seller"."seller_name"
ORDER BY
  "sum" DESC,
  "public"."kpi_seller"."seller_name" ASC
-----------------------------------------------------------
-- GMV por Loja
SELECT
  "public"."kpi_seller"."store_name" AS "store_name",
  SUM("public"."kpi_seller"."gmv") AS "sum"
FROM
  "public"."kpi_seller"
GROUP BY
  "public"."kpi_seller"."store_name"
ORDER BY
  "sum" DESC,
  "public"."kpi_seller"."store_name" ASC
-----------------------------------------------------------
-- Top Performers 
SELECT
  "public"."kpi_seller"."seller_name" AS "seller_name",
  SUM("public"."kpi_seller"."gmv") AS "sum"
FROM
  "public"."kpi_seller"
GROUP BY
  "public"."kpi_seller"."seller_name"
ORDER BY
  "sum" DESC,
  "public"."kpi_seller"."seller_name" ASC
LIMIT
  5
-----------------------------------------------------------
-- Bottom Performers
SELECT
  "public"."kpi_seller"."seller_name" AS "seller_name",
  SUM("public"."kpi_seller"."gmv") AS "sum"
FROM
  "public"."kpi_seller"
GROUP BY
  "public"."kpi_seller"."seller_name"
ORDER BY
  "sum" ASC,
  "public"."kpi_seller"."seller_name" ASC
LIMIT
  5
-----------------------------------------------------------
-- Meta vs Realizado
SELECT
  "public"."kpi_seller"."seller_name" AS "seller_name",
  SUM("public"."kpi_seller"."gmv") AS "sum",
  SUM("public"."kpi_seller"."target_gmv") AS "sum_2"
FROM
  "public"."kpi_seller"
GROUP BY
  "public"."kpi_seller"."seller_name"
ORDER BY
  "public"."kpi_seller"."seller_name" ASC


-- =========================================================
-- SEÇÃO: CONTROLADORIA
-- =========================================================

-- Margem Bruta
SELECT
  "source"."category" AS "category",
  "source"."sum" AS "sum",
  "source"."sum_2" AS "sum_2",
  CAST("source"."sum" AS DOUBLE PRECISION) / NULLIF(CAST("source"."sum_2" AS DOUBLE PRECISION), 0.0) AS "Margem Bruta"
FROM
  (
    SELECT
      "public"."kpi_category"."category" AS "category",
      SUM("public"."kpi_category"."gross_profit") AS "sum",
      SUM("public"."kpi_category"."net_revenue") AS "sum_2"
    FROM
      "public"."kpi_category"
    GROUP BY
      "public"."kpi_category"."category"
    ORDER BY
      "public"."kpi_category"."category" ASC
  ) AS "source"
LIMIT
  1048575
-----------------------------------------------------------
-- Impacto do Desconto 
SELECT
  "source"."channel" AS "channel",
  "source"."sum" AS "sum",
  "source"."sum_2" AS "sum_2",
  "source"."sum" - "source"."sum_2" AS "Impacto do Desconto"
FROM
  (
    SELECT
      "public"."kpi_channel"."channel" AS "channel",
      SUM("public"."kpi_channel"."gmv") AS "sum",
      SUM("public"."kpi_channel"."net_revenue") AS "sum_2"
    FROM
      "public"."kpi_channel"
    GROUP BY
      "public"."kpi_channel"."channel"
    ORDER BY
      "public"."kpi_channel"."channel" ASC
  ) AS "source"
LIMIT
  1048575
-----------------------------------------------------------
-- Custo
SELECT
  SUM("public"."kpi_category"."cogs") AS "sum"
FROM
  "public"."kpi_category"
-----------------------------------------------------------
-- Status do Pedido
SELECT
  "public"."kpi_status"."status" AS "status",
  SUM("public"."kpi_status"."order_count") AS "sum"
FROM
  "public"."kpi_status"
GROUP BY
  "public"."kpi_status"."status"
ORDER BY
  "public"."kpi_status"."status" ASC
-----------------------------------------------------------
-- Lucro por Categoria 
SELECT
  "public"."kpi_category"."category" AS "category",
  SUM("public"."kpi_category"."gross_profit") AS "sum"
FROM
  "public"."kpi_category"
GROUP BY
  "public"."kpi_category"."category"
ORDER BY
  "public"."kpi_category"."category" ASC
-----------------------------------------------------------
-- Lucro por Canal
SELECT
  "public"."kpi_channel"."channel" AS "channel",
  SUM("public"."kpi_channel"."gross_profit") AS "sum"
FROM
  "public"."kpi_channel"
GROUP BY
  "public"."kpi_channel"."channel"
ORDER BY
  "public"."kpi_channel"."channel" ASC