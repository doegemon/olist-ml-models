# Databricks notebook source
# MAGIC %pip install feature-engine scikit-plot mlflow

# COMMAND ----------

# DBTITLE 1,Bibliotecas
import mlflow
import pandas as pd
import scikitplot as skplt

from sklearn import tree
from sklearn import metrics
from sklearn import ensemble
from sklearn import pipeline
from sklearn import model_selection
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
to_remove = ['qtdRecencia', target] + var_identity
features = df_train.columns.tolist()
features = list(set(features) - set(to_remove))

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
model = tree.DecisionTreeClassifier(min_samples_leaf=50)

model_pipeline = pipeline.Pipeline([('Imputer -100', imputer_minus_100), ('Imputer 0', imputer_0), ('Decision Tree', model),])

# COMMAND ----------

model_pipeline.fit(X_train, y_train)

# COMMAND ----------

# DBTITLE 1,Verificando o Treino do Modelo
predict = model_pipeline.predict(X_train)
probas = model_pipeline.predict_proba(X_train)

# COMMAND ----------

skplt.metrics.plot_roc(y_train, probas)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_train, probas)

# COMMAND ----------

skplt.metrics.plot_lift_curve(y_train, probas)

# COMMAND ----------

# DBTITLE 1,Performance - Teste
predict_test = model_pipeline.predict(X_test)
probas_test = model_pipeline.predict_proba(X_test)

# COMMAND ----------

skplt.metrics.plot_roc(y_test, probas_test)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_test, probas_test)

# COMMAND ----------

# DBTITLE 1,Performance - OOT
predict_oot = model_pipeline.predict(df_oot[features])
probas_oot = model_pipeline.predict_proba(df_oot[features])

# COMMAND ----------

skplt.metrics.plot_roc(df_oot[target], probas_oot)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(df_oot[target], probas_oot)

# COMMAND ----------

# DBTITLE 1,Feature Importance
fs_importance = model_pipeline[-1].feature_importances_
fs_cols = model_pipeline[:-1].transform(X_train.head(1)).columns.tolist()

pd.Series(fs_importance, index=fs_cols).sort_values(ascending=False)

# COMMAND ----------

# DBTITLE 1,MLFlow
# Vinculando o modelo ao experimento
mlflow.set_experiment("/Users/email/olist-churn-doegemon")

# COMMAND ----------

with mlflow.start_run():
    
    mlflow.sklearn.autolog()

    imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitraty_number = -100, variables=missing_minus_100)
    imputer_0 = imputation.ArbitraryNumberImputer(arbitraty_number = 0, variables=missing_0)

    # model = tree.DecisionTreeClassifier(min_samples_leaf=50)
    model = ensemble.RandomForestClassifier(min_samples_leaf=50, n_jobs=-1, n_estimators=300, random_state=42)

    params = {"min_samples_leaf": [5, 10, 20, 50, 100], "n_estimators": [50, 100, 200, 300, 500]}

    grid = model_selection.GridSearchCV(model, params, cv=3, verbose=3, scoring='roc_auc')

    model_pipeline = pipeline.Pipeline([('Imputer -100', imputer_minus_100), ('Imputer 0', imputer_0), ('Grid Search', grid),])

    model_pipeline.fit(X_train, y_train)

    auc_train = metrics.roc_auc_score(y_train, model_pipeline.predict_proba(X_train)[:,1])
    auc_test = metrics.roc_auc_score(y_test, model_pipeline.predict_proba(X_test)[:,1])
    auc_oot = metrics.roc_auc_score(df_oot[target], model_pipeline.predict_proba(df_oot[features])[:,1])

    metrics_model = {"auc_train": auc_train, "auc_test": auc_test, "auc_oot": auc_oot}

    mlflow.log_metrics(metrics_model)
