import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.view(
  name = "settlements_stg"
)
def settlements_stg():
    df = spark.readStream.table("reconciliation.silver.settlements")
    df = df.withColumn("processed_timestamp", current_timestamp())
    return df

# Creating Empty Streaming Table 
dlt.create_streaming_table(name = "silver_settlements_view")


dlt.create_auto_cdc_flow(
  target = "silver_settlements_view",
  source = "settlements_stg",
  keys = ["settlement_id","transaction_id"],
  sequence_by = col("processed_timestamp"),
  stored_as_scd_type = 1
)

@dlt.view(
  name = "silver_settlements"
)

def silver_settlements():
   df = spark.readStream.table("settlements_stg")
   return df
 
dlt.create_streaming_table(name = "fact_settlements")

dlt.create_auto_cdc_flow(
  target = "fact_settlements",
  source = "silver_settlements",
  keys = ["settlement_id","transaction_id"],
  sequence_by = col("processed_timestamp"),
  stored_as_scd_type = 1,
  except_column_list =  ["processed_timestamp"]
)