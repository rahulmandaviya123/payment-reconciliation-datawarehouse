import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.view(
  name = "transactions_stg"
)
def transactions_stg():
    df = spark.readStream.table("reconciliation.silver.transactions")
    df = df.withColumn("processed_timestamp", current_timestamp())
    return df

# Creating Empty Streaming Table 
dlt.create_streaming_table(name = "silver_transactions_view")


dlt.create_auto_cdc_flow(
  target = "silver_transactions_view",
  source = "transactions_stg",
  keys = ["transaction_id"],
  sequence_by = col("processed_timestamp"),
  stored_as_scd_type = 1
)

@dlt.view(name = "silver_transactions")
def silver_transactions():
    df = spark.readStream.table("transactions_stg")
    return df
  
dlt.create_streaming_table(name = "fact_transactions")

dlt.create_auto_cdc_flow(
  target = "fact_transactions",
  source = "silver_transactions",
  keys = ["transaction_id"],
  sequence_by = col("processed_timestamp"),
  stored_as_scd_type = 1,
  except_column_list = ["processed_timestamp"]
)