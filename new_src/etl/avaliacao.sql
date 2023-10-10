-- Databricks notebook source
WITH tb_pedidos AS (
	SELECT DISTINCT t1.idPedido,
	                t2.idVendedor
	FROM silver.olist.pedido AS t1
	LEFT JOIN silver.olist.item_pedido AS t2
	ON t1.idPedido = t2.idPedido
	WHERE t1.dtPedido < '2018-01-01'
	AND t1.dtPedido >= add_months('2018-01-01', -6)
	AND t2.idVendedor IS NOT NULL
),
tb_join AS (
  SELECT t1.*,
  t2.vlNota
  FROM tb_pedidos AS t1
  LEFT JOIN silver.olist.avaliacao_pedido AS t2
  ON t1.idPedido = t2.idPedido
),
tb_summary AS (
  SELECT idVendedor, 
  AVG(vlNota) AS avgNota,
  PERCENTILE(vlNota, 0.5) as medianNota,
  MIN(vlNota) as minNOta,
  MAX(vlNota) as maxNota,
  COUNT(vlNota) / COUNT(idPedido) AS pctAvaliacao
  FROM tb_join
  GROUP BY 1
)
SELECT '2018-01-01' AS dtReference,
*
FROM tb_summary
