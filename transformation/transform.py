import pandas as pd
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import json
from pyspark.sql.functions import col, sum as sum_, count, avg, countDistinct, when

# Initialize AWS clients
s3_client = boto3.client("s3", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET_NAME = "lab6-realtime-ecommerce-pipelines"

# Initialize Spark
spark = (
    SparkSession.builder.appName("EcommerceKPITransformation")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)


def transform_file(file_key):
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
    df = pd.read_csv(local_file)
    spark_df = spark.createDataFrame(df)

    log_success(file_key, f"Transformation for {file_type} successful")


def log_success(file_key, message):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_key = f"logs/transform/{file_key.split('/')[-1]}_{timestamp}_success.log"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8"))


if __name__ == "__main__":
    event_string = os.environ.get("EVENT_DATA", "{}")
    event = json.loads(event_string)

    # Extract file key from the S3 event detail
    # This works because ResultPath preserves the original input
    file_key = event.get("detail", {}).get("object", {}).get("key", "")

    if file_key.startswith("input/"):
        transform_file(file_key)
