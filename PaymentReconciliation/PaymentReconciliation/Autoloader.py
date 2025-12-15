# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

import os
import sys

project_path = os.path.join(os.getcwd())

sys.path.append(project_path)
from util.Transformations import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## **User Transformations**

# COMMAND ----------

df_users = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "parquet")\
          .option("cloudFiles.schemaLocation", "abfss://silver@reconciliationdatalake.dfs.core.windows.net/dim_users/checkpoint")\
          .option("clouldFiles.schemaEvolutionMode", "addNewColumns")\
          .load("abfss://bronze@reconciliationdatalake.dfs.core.windows.net/dim_users/")

# COMMAND ----------

df_users = df_users.withColumn('user_key',sha2(concat_ws("||", "user_id","user_name"), 256))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Write Data to External location**

# COMMAND ----------

# Write Data To External Storage(data lake)

df_users.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@reconciliationdatalake.dfs.core.windows.net/dim_users/checkpoint")\
    .option("mergeSchema", "true")\
    .trigger(once=True)\
    .start("abfss://silver@reconciliationdatalake.dfs.core.windows.net/dim_users/data/")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS reconciliation.silver.dim_users
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@reconciliationdatalake.dfs.core.windows.net/dim_users/data/';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Merchants Tranformations**

# COMMAND ----------

df_merchants = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "parquet")\
          .option("cloudFiles.schemaLocation", "abfss://silver@reconciliationdatalake.dfs.core.windows.net/dim_merchants/checkpoint")\
          .option("clouldFiles.schemaEvolutionMode", "addNewColumns")\
          .load("abfss://bronze@reconciliationdatalake.dfs.core.windows.net/dim_merchants/")

# COMMAND ----------

df_merchants = df_merchants.withColumn('merchant_key',sha2(concat_ws(":", "merchant_id"), 256))

# COMMAND ----------

# Write Data To External Storage(data lake)

df_merchants.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@reconciliationdatalake.dfs.core.windows.net/dim_merchants/checkpoint")\
    .option("mergeSchema", "true")\
    .trigger(once=True)\
    .start("abfss://silver@reconciliationdatalake.dfs.core.windows.net/dim_merchants/data/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS reconciliation.silver.dim_merchants
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@reconciliationdatalake.dfs.core.windows.net/dim_merchants/data/';

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Fact_Settlements**

# COMMAND ----------

df_settlements = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "parquet")\
          .option("cloudFiles.schemaLocation", "abfss://silver@reconciliationdatalake.dfs.core.windows.net/settlements/checkpoint")\
          .option("clouldFiles.schemaEvolutionMode", "addNewColumns")\
          .load("abfss://bronze@reconciliationdatalake.dfs.core.windows.net/settlements/")

# COMMAND ----------

df_settlements.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@reconciliationdatalake.dfs.core.windows.net/settlements/checkpoint")\
    .option("mergeSchema", "true")\
    .trigger(once=True)\
    .start("abfss://silver@reconciliationdatalake.dfs.core.windows.net/settlements/data/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS reconciliation.silver.settlements
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@reconciliationdatalake.dfs.core.windows.net/settlements/data/';

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Fact_Transactions**

# COMMAND ----------

df_transactions = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "parquet")\
          .option("cloudFiles.schemaLocation", "abfss://silver@reconciliationdatalake.dfs.core.windows.net/transactions/checkpoint")\
          .option("clouldFiles.schemaEvolutionMode", "addNewColumns")\
          .load("abfss://bronze@reconciliationdatalake.dfs.core.windows.net/transactions/")

# COMMAND ----------


df_transactions.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@reconciliationdatalake.dfs.core.windows.net/transactions/checkpoint")\
    .option("mergeSchema", "true")\
    .trigger(once=True)\
    .start("abfss://silver@reconciliationdatalake.dfs.core.windows.net/transactions/data/")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS reconciliation.silver.transactions
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@reconciliationdatalake.dfs.core.windows.net/transactions/data/';