import os
import json
import pandas as pd
import boto3
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, count, avg, countDistinct, when

#  Setup Logging
logging.basicConfig(level=logging.INFO)

#  Initialize AWS clients
s3_client = boto3.client("s3", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET_NAME = "lab6-realtime-ecommerce-pipelines"

#  Initialize Spark with proper hostname config
try:
    spark = (
        SparkSession.builder.appName("EcommerceKPITransformation")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.hostname", "localhost")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    logging.info(" SparkSession created successfully.")
except Exception as e:
    logging.error(f" Failed to create SparkSession: {e}")
    raise


#  Transformation logic
def transform_file(file_key):
    file_name = file_key.split("/")[-1]
    if "order_items" in file_name:
        file_type = "order_items"
    elif "products" in file_name:
        file_type = "products"
    else:
        file_type = file_name.split("_")[0]

    # Download and read the file
    local_file = f"/tmp/{file_name}"
    s3_client.download_file(BUCKET_NAME, file_key, local_file)

    # Convert to Spark DataFrame
    df = pd.read_csv(local_file)
    spark_df = spark.createDataFrame(df)

    # You can add transformation logic here
    log_success(file_key, f"Transformation for {file_type} successful")


#  Logging to S3
def log_success(file_key, message):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_key = f"logs/transform/{file_key.split('/')[-1]}_{timestamp}_success.log"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8"))


#  Entry point
if __name__ == "__main__":
    event_string = os.environ.get("EVENT_DATA", "{}")
    event = json.loads(event_string)
    file_key = event.get("detail", {}).get("object", {}).get("key", "")
    if file_key.startswith("input/"):
        transform_file(file_key)
