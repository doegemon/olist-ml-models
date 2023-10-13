-- DROP TABLE IF EXISTS silver.analytics.fs_vendedor_entrega;
-- CREATE TABLE silver.analytics.fs_vendedor_entrega

WITH tb_pedido AS (
  SELECT DISTINCT t1.idPedido,
  t2.idVendedor,
  t1.descSituacao, 
  t1.dtPedido,
  t1.dtAprovado,
  t1.dtEntregue,
  t1.dtEstimativaEntrega,
  SUM(vlFrete) AS totalFrete
  FROM silver.olist.pedido AS t1
  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido
  WHERE t1.dtPedido < '{date}'
  AND t1.dtPedido >= add_months('{date}', -6)
  AND t2.idVendedor IS NOT NULL
  GROUP BY t1.idPedido,
  t2.idVendedor,
  t1.descSituacao, 
  t1.dtPedido,
  t1.dtAprovado,
  t1.dtEntregue,
  t1.dtEstimativaEntrega
),
tb_summary AS (
  SELECT idVendedor,
  COUNT(DISTINCT CASE WHEN DATE(COALESCE(dtEntregue, '{date}')) > DATE(dtEstimativaEntrega) THEN idPedido END) / COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pctPedidoAtraso,
  COUNT(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoCancelado,
  AVG(totalFrete) as avgFrete,
  PERCENTILE(totalFrete, 0.5) as medianFrete,
  MAX(totalFrete) as maxFrete,
  MIN(totalFrete) as minFrete,
  AVG(DATEDIFF(COALESCE(dtEntregue, '{date}'), dtAprovado)) AS qtdDiasAprovadoEntrega,
  AVG(DATEDIFF(COALESCE(dtEntregue, '{date}'), dtPedido)) AS qtdDiasPedidoEntrega,
  AVG(DATEDIFF(dtEstimativaEntrega, COALESCE(dtEntregue, '{date}'))) AS qtdDiasEntregaPromessa
  FROM tb_pedido
  GROUP BY idVendedor
)
SELECT '{date}' as dtReference, 
NOW() AS dtIngestion,
*
FROM tb_summary