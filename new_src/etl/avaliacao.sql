-- DROP TABLE IF EXISTS silver.analytics.fs_vendedor_avaliacao;
-- CREATE TABLE silver.analytics.fs_vendedor_avaliacao

WITH tb_pedidos AS (
	SELECT DISTINCT t1.idPedido,
	                t2.idVendedor
	FROM silver.olist.pedido AS t1
	LEFT JOIN silver.olist.item_pedido AS t2
	ON t1.idPedido = t2.idPedido
	WHERE t1.dtPedido < '{date}'
	AND t1.dtPedido >= add_months('{date}', -6)
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
SELECT '{date}' AS dtReference,
NOW() as dtIngestion,
*
FROM tb_summary