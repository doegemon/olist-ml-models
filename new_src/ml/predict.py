# Databricks notebook source
# MAGIC %pip install feature-engine scikit-plot mlflow

# COMMAND ----------

import mlflow
import datetime

# COMMAND ----------

model = mlflow.sklearn.load_model("models:/Olist Vendedor Churn/Production")

# COMMAND ----------

# DBTITLE 1,Predict
df = spark.table("silver.analytics.abt_olist_churn").toPandas()
predict = model.predict_proba(df[model.feature_names_in])
predict_0 = predict[:, 0]
predict_1 = predict[:, 1]

# COMMAND ----------

# DBTITLE 1,ETL
df_extract = df[['idVendedor']].copy()
df_extract['0'] = predict_0
df_extract['1'] = predict_1

df_extract = df_extract.set_index('idVendedor').stack().reset_index()
df_extract.columns = ['idVendedor', 'descClass', 'Score']
df_extract['descModel'] = 'Churn_Vendedor'
dt_now = datetime.datetime.now()
df_extract['dtScore'] = df['dtReference'][0]
df_extract['dtIngestion'] = dt_now.strftime('%Y-%m-%d')

# COMMAND ----------

# DBTITLE 1,Results
df_spark = spark.createDataFrame(df_extract)
df_spark.display()
