import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.view(
  name = "dim_users_stg"
)
def dim_users_stg():
    df = spark.readStream.table("reconciliation.silver.dim_users")
    df = df.withColumn("processed_timestamp", current_timestamp())
    return df

# Creating Empty Streaming Table 
dlt.create_streaming_table(name = "silver_users")


dlt.create_auto_cdc_flow(
  target = "silver_users",
  source = "dim_users_stg",
  keys = ["user_id"],
  sequence_by = col("processed_timestamp"),
  stored_as_scd_type = 1
)


@dlt.view(
  name = "users_glod_view"
)

def users_glod_view():
    df = spark.readStream.table("dim_users_stg")
    return df
  
dlt.create_streaming_table(name = "dim_users")

dlt.create_auto_cdc_flow(
  target = "dim_users",
  source = "users_glod_view",
  keys = ["user_id"],
  sequence_by = col("processed_timestamp"),
  stored_as_scd_type = 2,
  except_column_list = ["processed_timestamp"]
)