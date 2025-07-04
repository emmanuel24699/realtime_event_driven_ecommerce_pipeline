import json
import os
import boto3
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Initialize AWS clients
s3_client = boto3.client("s3", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

BUCKET_NAME = "lab6-realtime-ecommerce-pipelines"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:985539772768:lab6-pipeline-failure-notifications"


def log_error(file_key, message):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_key = f"logs/stage/{file_key.split('/')[-1]}_{timestamp}_error.log"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8"))
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=f"Staging failed for {file_key}: {message}",
        Subject="Pipeline Staging Failure",
    )


def stage_file(file_key):
    spark = SparkSession.builder.appName("EcommerceStaging").getOrCreate()
    file_name = file_key.split("/")[-1]
    file_type = "order_items" if "order_items" in file_name else file_name.split("_")[0]

    try:
        # Download file from S3
        local_file = f"/tmp/{file_name}"
        s3_client.download_file(BUCKET_NAME, file_key, local_file)

        # Read CSV into Spark DataFrame
        df = spark.read.csv(local_file, header=True, inferSchema=True)

        # Add partition column
        if file_type in ["orders", "order_items"]:
            df = df.withColumn("order_date", to_date(col("created_at")))

        # Write to Delta Lake
        output_path = (
            f"s3://lab6-realtime-ecommerce-pipelines/staging/fact/{file_type}/"
        )
        if file_type == "products":
            df.write.format("delta").mode("overwrite").save(output_path)
        else:
            df.write.format("delta").mode("append").partitionBy("order_date").save(
                output_path
            )

        # Log success
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_key = f"logs/stage/{file_name}_{timestamp}_success.log"
        s3_client.put_object(
            Bucket=BUCKET_NAME, Key=log_key, Body="Staging successful".encode("utf-8")
        )

        return True
    except Exception as e:
        log_error(file_key, str(e))
        return False


if __name__ == "__main__":
    event_string = os.environ.get("EVENT_DATA", "{}")
    try:
        event = json.loads(event_string)
    except json.JSONDecodeError as e:
        print(f"Error parsing EVENT_DATA: {str(e)}", file=sys.stderr)
        sys.exit(1)

    file_key = event.get("detail", {}).get("object", {}).get("key", "")
    if not file_key.startswith("input/"):
        print(f"Invalid file key: {file_key}", file=sys.stderr)
        sys.exit(1)

    success = stage_file(file_key)
    sys.exit(0 if success else 1)
