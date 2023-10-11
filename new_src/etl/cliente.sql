-- Databricks notebook source
-- DROP TABLE IF EXISTS silver.analytics.fs_vendedor_cliente;
-- CREATE TABLE silver.analytics.fs_vendedor_cliente

WITH tb_pedidos AS (
	SELECT DISTINCT t1.idPedido,
			            t1.idCliente,
	                t2.idVendedor,
			            t3.descUF
	FROM silver.olist.pedido AS t1
	LEFT JOIN silver.olist.item_pedido AS t2
	ON t1.idPedido = t2.idPedido
	LEFT JOIN silver.olist.cliente AS t3
	ON t1.idCliente = t3.idCliente
	WHERE t1.dtPedido < '2018-01-01'
	AND t1.dtPedido >= add_months('2018-01-01', -6)
	AND t2.idVendedor IS NOT NULL
),
tb_group AS (
  SELECT idVendedor,
  count(DISTINCT descUF) as qtdUFsPedidos,
  count(DISTINCT CASE WHEN descUF = 'AC' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoAC,
  count(DISTINCT CASE WHEN descUF = 'AL' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoAL,
  count(DISTINCT CASE WHEN descUF = 'AM' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoAM,
  count(DISTINCT CASE WHEN descUF = 'AP' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoAP,
  count(DISTINCT CASE WHEN descUF = 'BA' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoBA,
  count(DISTINCT CASE WHEN descUF = 'CE' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoCE,
  count(DISTINCT CASE WHEN descUF = 'DF' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoDF,
  count(DISTINCT CASE WHEN descUF = 'ES' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoES,
  count(DISTINCT CASE WHEN descUF = 'GO' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoGO,
  count(DISTINCT CASE WHEN descUF = 'MA' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoMA,
  count(DISTINCT CASE WHEN descUF = 'MG' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoMG,
  count(DISTINCT CASE WHEN descUF = 'MS' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoMS,
  count(DISTINCT CASE WHEN descUF = 'MT' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoMT,
  count(DISTINCT CASE WHEN descUF = 'PA' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoPA,
  count(DISTINCT CASE WHEN descUF = 'PB' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoPB,
  count(DISTINCT CASE WHEN descUF = 'PE' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoPE,
  count(DISTINCT CASE WHEN descUF = 'PI' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoPI,
  count(DISTINCT CASE WHEN descUF = 'PR' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoPR,
  count(DISTINCT CASE WHEN descUF = 'RJ' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoRJ,
  count(DISTINCT CASE WHEN descUF = 'RN' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoRN,
  count(DISTINCT CASE WHEN descUF = 'RO' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoRO,
  count(DISTINCT CASE WHEN descUF = 'RR' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoRR,
  count(DISTINCT CASE WHEN descUF = 'RS' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoRS,
  count(DISTINCT CASE WHEN descUF = 'SC' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoSC,
  count(DISTINCT CASE WHEN descUF = 'SE' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoSE,
  count(DISTINCT CASE WHEN descUF = 'SP' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoSP,
  count(DISTINCT CASE WHEN descUF = 'TO' THEN idPedido END) / COUNT(DISTINCT idPedido) as pctPedidoTO
  FROM tb_pedidos
  GROUP BY 1
)
SELECT '2018-01-01' AS dtReference,
       *
FROM tb_group
