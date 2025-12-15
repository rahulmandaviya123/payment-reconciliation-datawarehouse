import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.view(
  name = "dim_merchants_stg"
)
def dim_merchants_stg():
    df = spark.readStream.table("reconciliation.silver.dim_merchants")
    df = df.withColumn("processed_timestamp", current_timestamp())
    return df

# Creating Empty Streaming Table 
dlt.create_streaming_table(name = "silver_merchants")


dlt.create_auto_cdc_flow(
  target = "silver_merchants",
  source = "dim_merchants_stg",
  keys = ["merchant_id"],
  sequence_by = col("processed_timestamp"),
  stored_as_scd_type = 1
)

@dlt.view(
  name = "merchants_gold_view"
)

def merchants_gold_view():
    df = spark.readStream.table("dim_merchants_stg")
    return df

dlt.create_streaming_table(name = "dim_merchants")

dlt.create_auto_cdc_flow(
  target = "dim_merchants",
  source = "merchants_gold_view",
  keys = ["merchant_id"],
  sequence_by = col("processed_timestamp"),
  stored_as_scd_type = 2,
  except_column_list = ["processed_timestamp"]
)