-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ETL das Variáveis de Pagamento

-- COMMAND ----------

-- Query de teste
SELECT *
FROM silver.olist.pagamento_pedido
LIMIT 100

-- COMMAND ----------

-- Verificando a quantidade de pedidos por dia (descobrir o período dos dados coletados)
SELECT date(dtPedido) as dtPedido, 
       count(*) as qtPedido
FROM silver.olist.pedido
GROUP BY 1
ORDER BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Período de dados que vamos utilizar: 2018-01-01 como 'data atual' e utilizar os dados dos últimos 6 meses para cálculo das variáveis

-- COMMAND ----------

-- Aplicando esse filtro de período
SELECT *
FROM silver.olist.pedido
WHERE dtPedido < '2018-01-01'
AND dtPedido >= add_months('2018-01-01', -6)

-- COMMAND ----------

-- Cruzando as informações de pedido com informações de pagamento + obtenção do id do Vendedor
SELECT t3.idVendedor,
       t2.*
FROM silver.olist.pedido AS t1
LEFT JOIN silver.olist.pagamento_pedido AS t2
ON t1.idPedido = t2.idPedido
LEFT JOIN silver.olist.item_pedido AS t3
ON t1.idPedido = t3.idPedido
WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)

-- COMMAND ----------

-- Criando uma tabela temporária/uma view para facilitar a criação das variáveis
WITH tb_join AS (
	SELECT t3.idVendedor,
	       t2.*
	FROM silver.olist.pedido AS t1
	LEFT JOIN silver.olist.pagamento_pedido AS t2
	ON t1.idPedido = t2.idPedido
	LEFT JOIN silver.olist.item_pedido AS t3
	ON t1.idPedido = t3.idPedido
	WHERE t1.dtPedido < '2018-01-01'
	AND t1.dtPedido >= add_months('2018-01-01', -6)
	AND t3.idVendedor IS NOT NULL
)
SELECT idVendedor,
       descTipoPagamento,
       COUNT(DISTINCT idPedido) AS qtdePedidoMeioPagamento,
       SUM(vlPagamento) AS vlPedidoMeioPagamento
FROM tb_join
GROUP BY 1,2
ORDER BY 1,2

-- COMMAND ----------

-- Criando as variáveis como colunas de uma tabela
WITH tb_join AS (
	SELECT t3.idVendedor,
	       t2.*
	FROM silver.olist.pedido AS t1
	LEFT JOIN silver.olist.pagamento_pedido AS t2
	ON t1.idPedido = t2.idPedido
	LEFT JOIN silver.olist.item_pedido AS t3
	ON t1.idPedido = t3.idPedido
	WHERE t1.dtPedido < '2018-01-01'
	AND t1.dtPedido >= add_months('2018-01-01', -6)
	AND t3.idVendedor IS NOT NULL
),
tb_group AS (
	SELECT idVendedor,
	       descTipoPagamento,
	       COUNT(DISTINCT idPedido) AS qtdePedidoMeioPagamento,
	       SUM(vlPagamento) AS vlPedidoMeioPagamento
	FROM tb_join
	GROUP BY 1,2
	ORDER BY 1,2
)
SELECT idVendedor,
       SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_boleto_pedido,
       SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_credit_card_pedido,
       SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_voucher_pedido,
       SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_debit_card_pedido,

       SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_qtde_boleto_pedido,
       SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_qtde_credit_card_pedido,
       SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_qtde_voucher_pedido,
       SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_qtde_debit_card_pedido,

       SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_boleto_pedido,
       SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_credit_card_pedido,
       SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_voucher_pedido,
       SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_debit_card_pedido,

       SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
       SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
       SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
       SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido
FROM tb_group
GROUP BY 1

-- COMMAND ----------

-- Criando a tabela com pedidos distintos por vendedor no período desejado
SELECT DISTINCT t1.idPedido,
                t2.idVendedor
FROM silver.olist.pedido AS t1
LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido
WHERE t1.dtPedido < '2018-01-01'
AND t1.dtPedido >= add_months('2018-01-01', -6)
AND t2.idVendedor IS NOT NULL

-- COMMAND ----------

-- Criando todas as variáveis de pagamento propostas
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
	SELECT t1.idVendedor,
	       t2.*
	FROM tb_pedidos AS t1
	LEFT JOIN silver.olist.pagamento_pedido AS t2
	ON t1.idPedido = t2.idPedido
),
tb_group AS (
	SELECT idVendedor,
	       descTipoPagamento,
	       COUNT(DISTINCT idPedido) AS qtdePedidoMeioPagamento,
	       SUM(vlPagamento) AS vlPedidoMeioPagamento
	FROM tb_join
	GROUP BY 1,2
	ORDER BY 1,2
),
tb_meios AS (
	SELECT idVendedor,
       	SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_boleto_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_credit_card_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_voucher_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_debit_card_pedido,

       	SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_qtde_boleto_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_qtde_credit_card_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_qtde_voucher_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) / SUM(qtdePedidoMeioPagamento) as pct_qtde_debit_card_pedido,

       	SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_boleto_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_credit_card_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_voucher_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) as valor_debit_card_pedido,

       	SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
       	SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) / SUM(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido
	FROM tb_group
	GROUP BY idVendedor
),
tb_cartao AS (
	SELECT idVendedor,
	       AVG(nrParcelas) AS avgQtdeParcelas,
	       PERCENTILE(nrParcelas, 0.5) AS medianQtdeParcelas,
	       MAX(nrParcelas) AS maxQtdeParcelas,
	       MIN(nrParcelas) AS minQtdeParcelas
	FROM tb_join
	WHERE descTipoPagamento = 'credit_card'
	GROUP BY idVendedor
)
SELECT '2018-01-01' AS dtReference,
        t1.*,
        t2.avgQtdeParcelas,
        t2.medianQtdeParcelas,
        t2.maxQtdeParcelas,
        t2.minQtdeParcelas
FROM tb_meios AS t1
LEFT JOIN tb_cartao AS t2
ON t1.idVendedor = t2.idVendedor
