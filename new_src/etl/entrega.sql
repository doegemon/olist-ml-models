-- Databricks notebook source
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
  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
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
  COUNT(DISTINCT CASE WHEN DATE(COALESCE(dtEntregue, '2018-01-01')) > DATE(dtEstimativaEntrega) THEN idPedido END) / COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pctPedidoAtraso,
  COUNT(DISTINCT CASE WHEN descSituacao = 'canceled' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoCancelado,
  AVG(totalFrete) as avgFrete,
  PERCENTILE(totalFrete, 0.5) as medianFrete,
  MAX(totalFrete) as maxFrete,
  MIN(totalFrete) as minFrete,
  AVG(DATEDIFF(COALESCE(dtEntregue, '2018-01-01'), dtAprovado)) AS qtdDiasAprovadoEntrega,
  AVG(DATEDIFF(COALESCE(dtEntregue, '2018-01-01'), dtPedido)) AS qtdDiasPedidoEntrega,
  AVG(DATEDIFF(dtEstimativaEntrega, COALESCE(dtEntregue, '2018-01-01'))) AS qtdDiasEntregaPromessa
  FROM tb_pedido
  GROUP BY idVendedor
)
SELECT '2018-01-01' as dtReference, 
*
FROM tb_summary
