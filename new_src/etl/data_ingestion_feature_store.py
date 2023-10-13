# Databricks notebook source
import datetime
from tqdm import tqdm

# função para abrir um arquivo e importar o conteúdo desse arquivo
def import_query(path): 
  with open(path, 'r') as open_file:
    return open_file.read()

# função para verificar se a tabela já existe na database
def table_exists(database, table):
  count = (spark.sql(f"SHOW TABLES FROM {database}").filter(f"tableName = '{table}'"))
  return count > 0

# função para criar uma lista de datas para usar como referência na coleta de dados e criação das tabelas
def date_range(dt_start, dt_stop, period='daily'):
  datetime_start = datetime.datetime.strptime(dt_start, '%Y-%m-%d')
  datetime_stop = datetime.datetime.strptime(dt_stop, '%Y-%m-%d')
  dates = []
  
  while datetime_start <= datetime_stop:
    dates.append(datetime_start.strftime('%Y-%m-%d'))
    datetime_start += datetime.timedelta(days=1)

  if period == 'daily':
    return dates
  elif period == 'monthly':
    return [i for i in dates if i.endswith('01')]


table = dbutils.widgets.get("table")
table_name = f"fs_vendedor_{table}"
database = "silver.analytics"
period = dbutils.widgets.get("period")'

query = import_query(f"{table}.sql")

date_start = dbutils.widgets.get("date_start")
date_stop = dbutils.widgets.get("date_stop")
dates = date_range(date_start, date_stop, period)

# criando a tabela ou atualizando de forma incremental com novas 'safras'
if not table_exists(database, table_name):
  print("Criando a tabela...")
  (spark.sql(query.format(date=dates.pop(0)))
                .coalesce(1)
                .write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionby("dtReference")
                .saveAsTable(f"{database}.{table_name}"))

else:
  print("Atualizando a tabela...")
  for i in tqdm(dates):
    spark.sql(f"DELETE FROM {database}.{table_name} WHERE dtReference = '{i}'")
    (spark.sql(query.format(date=i))
                    .coalesce(1)
                    .write.format("delta")
                    .mode("append")
                    .saveAsTable(f"{database}.{table_name}"))
