# Auto Loader: Read CSV files and write to bronze table
from pyspark.sql.functions import current_timestamp

# Source and target configuration
source_path = "/Volumes/retail_q/volumes/blob_source/transactions_source/"
target_table = "retail_q.blob_bronze.transactions"
checkpoint_path = "/Volumes/retail_q/volumes/blob_source/_checkpoints/transactions"

# Read CSV files using Auto Loader
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .option("header", "true")
  .option("inferSchema", "true")
  .load(source_path)
  .withColumn("ingestion_timestamp", current_timestamp())
)

# Write to bronze Delta table
(df.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .trigger(availableNow=True)
  .toTable(target_table)
)
