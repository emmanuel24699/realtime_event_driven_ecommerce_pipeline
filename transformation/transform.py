import pandas as pd
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import json
from pyspark.sql.functions import col, sum as sum_, count, avg, countDistinct

# Initialize AWS clients
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
BUCKET_NAME = "lab6-realtime-ecommerce-pipelines"

# Initialize Spark with Delta Lake
spark = (
    SparkSession.builder.appName("EcommerceKPITransformation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
