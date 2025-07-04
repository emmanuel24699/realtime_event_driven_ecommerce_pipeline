import pandas as pd
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import json
from pyspark.sql.functions import col
import os

# Initialize AWS clients
s3_client = boto3.client("s3", region_name="us-east-1")
BUCKET_NAME = "lab6-realtime-ecommerce-pipelines"

# Initialize Spark with Delta Lake
spark = (
    SparkSession.builder.appName("EcommerceStaging")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)


def stage_file(file_key):
    # Determine file type
    file_name = file_key.split("/")[-1]
    if "order_items" in file_name:
        file_type = "order_items"
    elif "products" in file_name:
        file_type = "products"
    else:
        file_type = file_name.split("_")[0]

    # Download file from S3
    local_file = f"/tmp/{file_name}"
    s3_client.download_file(BUCKET_NAME, file_key, local_file)

    # Read CSV and convert to Spark DataFrame
    try:
        df = pd.read_csv(local_file)
        spark_df = spark.createDataFrame(df)
        if file_type != "products":
            spark_df = spark_df.withColumn("order_date", col("created_at").cast("date"))
    except Exception as e:
        log_error(file_key, f"Failed to stage file: {str(e)}")
        raise e

    # Merge into Delta Lake fact table
    delta_path = f"s3a://{BUCKET_NAME}/staging/fact/{file_type}"
    key = "id" if file_type in ["products", "order_items"] else "order_id"

    if not DeltaTable.isDeltaTable(spark, delta_path):
        if file_type == "products":
            spark_df.write.format("delta").mode("overwrite").save(delta_path)
        else:
            spark_df.write.format("delta").partitionBy("order_date").mode(
                "overwrite"
            ).save(delta_path)

    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.alias("target").merge(
        spark_df.alias("source"), f"target.{key} = source.{key}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    log_success(file_key, "Staging successful")


def log_error(file_key, message):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_key = f"logs/stage/{file_key.split('/')[-1]}_{timestamp}_error.log"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8"))


def log_success(file_key, message):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_key = f"logs/stage/{file_key.split('/')[-1]}_{timestamp}_success.log"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8"))


if __name__ == "__main__":
    event_string = os.environ.get("EVENT_DATA", "{}")
    event = json.loads(event_string)
    file_key = event.get("detail", {}).get("object", {}).get("key", "")
    if file_key.startswith("input/"):
        stage_file(file_key)
