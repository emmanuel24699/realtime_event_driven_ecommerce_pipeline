import pandas as pd
import boto3
import os
from datetime import datetime
import json

# Initialize AWS clients
s3_client = boto3.client("s3", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

BUCKET_NAME = "lab6-realtime-ecommerce-pipelines"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:985539772768:lab6-pipeline-failure-notifications"


def validate_file(file_key):
    # Define required columns
    required_columns = {
        "orders": ["order_id", "user_id", "status", "created_at", "num_of_item"],
        "products": ["id", "category", "retail_price"],
        "order_items": [
            "id",
            "order_id",
            "user_id",
            "product_id",
            "status",
            "created_at",
            "sale_price",
        ],
    }

    # Determine file type
    file_type = (
        file_key.split("/")[-1].split("_")[0]
        if "order_items" not in file_key
        else "order_items"
    )
    file_type = "products" if "products" in file_key else file_type

    # Download file from S3
    local_file = f"/tmp/{file_key.split('/')[-1]}"
    s3_client.download_file(BUCKET_NAME, file_key, local_file)

    # Read CSV
    try:
        df = pd.read_csv(local_file)
    except Exception as e:
        log_error(file_key, f"Failed to read CSV: {str(e)}")
        move_to_rejected(file_key)
        return False

    # Check required columns
    missing_columns = [
        col for col in required_columns.get(file_type, []) if col not in df.columns
    ]
    if missing_columns:
        log_error(file_key, f"Missing columns: {missing_columns}")
        move_to_rejected(file_key)
        return False

    # Validate data types
    try:
        if file_type == "orders":
            df["order_id"] = df["order_id"].astype(int)
            df["num_of_item"] = df["num_of_item"].astype(int)
            pd.to_datetime(df["created_at"])
        elif file_type == "products":
            df["id"] = df["id"].astype(int)
            df["retail_price"] = df["retail_price"].astype(float)
        elif file_type == "order_items":
            df["id"] = df["id"].astype(int)
            df["order_id"] = df["order_id"].astype(int)
            df["product_id"] = df["product_id"].astype(int)
            df["sale_price"] = df["sale_price"].astype(float)
            pd.to_datetime(df["created_at"])
    except Exception as e:
        log_error(file_key, f"Data type validation failed: {str(e)}")
        move_to_rejected(file_key)
        return False

    # Log success
    log_success(file_key, "Validation successful")
    return True


def log_error(file_key, message):
    log_to_s3(file_key, message, "error")
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=f"Validation failed for {file_key}: {message}",
        Subject="Pipeline Validation Failure",
    )


def log_success(file_key, message):
    log_to_s3(file_key, message, "success")


def log_to_s3(file_key, message, status):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_key = f"logs/validate/{file_key.split('/')[-1]}_{timestamp}_{status}.log"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8"))


def move_to_rejected(file_key):
    s3_client.copy_object(
        Bucket=BUCKET_NAME,
        CopySource={"Bucket": BUCKET_NAME, "Key": file_key},
        Key=f"rejected/{file_key.split('/')[-1]}",
    )
    s3_client.delete_object(Bucket=BUCKET_NAME, Key=file_key)


if __name__ == "__main__":
    event = json.loads(os.environ.get("EVENT_DATA", "{}"))
    file_key = (
        event.get("Records", [{}])[0].get("s3", {}).get("object", {}).get("key", "")
    )
    if file_key.startswith("input/"):
        validate_file(file_key)
