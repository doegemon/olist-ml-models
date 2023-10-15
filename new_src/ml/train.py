# Databricks notebook source
# DBTITLE 1,Bibliotecas
import pandas as pd
from sklearn import model_selection
from sklearn import tree
from sklearn import pipeline
from feature_engine import imputation

# COMMAND ----------

# DBTITLE 1,Conjuntos de Dados
df = spark.table("silver.analytics.abt_olist_churn").toPandas()
df_oot = df[df['dtReference'] == '2018-01-01']
df_train = df[df['dtReference'] != '2018-01-01']

# COMMAND ----------

# MAGIC %md 
# MAGIC Out of Time = a última safra de dados que vai servir de conjunto de dados de teste (simula os dados e o ambiente em produção)

# COMMAND ----------

# DBTITLE 1,Definindo Variáveis
var_identity = ['dtReference', 'idVendedor']
var_target = 'flChurn'
features = df_train.columns.tolist()
features = list(set(features) - set(var_identity + [target]))

# COMMAND ----------

# DBTITLE 1,Divisão entre Treino e Teste
X_train, X_test, y_train, y_test = model_selection.train_test_split(df_train[features], df_train[var_target], train_size = 0.8, random_state = 42)

# Verificando se tivemos uma amostra enviesada da variável resposta
print("Proporção da Variável Resposta no Treino:" y_train.mean())
print("Proporção da Variável Resposta no Teste:" y_test.mean())

# COMMAND ----------

# DBTITLE 1,Explorando os Dados
X_train.describe()

# COMMAND ----------

X_train.isna().sum().sort_values(ascending=False)

# COMMAND ----------

# DBTITLE 1,Transformando os Dados
missing_minus_100 = ['avgIntervaloVendas', 'maxNota', 'medianNota', 'minNota', 'avgNota', 'avgVolumeProduto', 'minVolumeProduto', 'maxVolumeProduto', 'medianVolumeProduto',]

missing_0 = ['medianQtdeParcelas', 'avgQtdeParcelas', 'minQtdeParcelas', 'maxQtdeParcelas',]

imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitraty_number = -100, variables=missing_minus_100)

imputer_0 = imputation.ArbitraryNumberImputer(arbitraty_number = 0, variables=missing_0)

# COMMAND ----------

# DBTITLE 1,Modelos
model = tree.DecisionTreeClassifier()

model_pipeline = pipeline.Pipeline([('Imputer -100', imputer_minus_100), ('Imputer 0', imputer_0), ('Decision Tree', model),])

# COMMAND ----------

model_pipeline.fit(X_train, y_train)

# COMMAND ----------

model_pipeline.predict(X_test)

# COMMAND ----------

model_pipeline.predict(df_oot[features])
