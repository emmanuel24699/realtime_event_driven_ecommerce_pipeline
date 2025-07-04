import boto3
from datetime import datetime
from pyspark.sql import SparkSession
import json

# Initialize AWS clients
s3_client = boto3.client("s3", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET_NAME = "lab6-realtime-ecommerce-pipelines"

# Initialize Spark
spark = SparkSession.builder.appName("EcommerceDynamoDBWrite").getOrCreate()


def write_dynamodb(file_key):
    # Only process order_items files
    if "order_items" not in file_key:
        log_success(file_key, "No DynamoDB write for non-order_items file")
        return

    # Read KPIs from S3
    try:
        category_kpis = spark.read.parquet(f"s3a://{BUCKET_NAME}/staging/kpis/category")
        order_kpis = spark.read.parquet(f"s3a://{BUCKET_NAME}/staging/kpis/order")
    except Exception as e:
        log_error(file_key, f"Failed to read KPIs: {str(e)}")
        raise e

    # Write to DynamoDB
    try:
        write_to_table(category_kpis, "CategoryKPIs", ["category", "order_date"])
        write_to_table(order_kpis, "OrderKPIs", ["order_date"])
    except Exception as e:
        log_error(file_key, f"Failed to write to DynamoDB: {str(e)}")
        raise e

    log_success(file_key, "DynamoDB write successful")


def write_to_table(df, table_name, key_columns):
    table = dynamodb.Table(table_name)
    for row in df.collect():
        item = {k: v for k, v in row.asDict().items() if v is not None}
        try:
            table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(" + key_columns[0] + ")",
            )
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            table.update_item(
                Key={k: item[k] for k in key_columns},
                UpdateExpression="SET "
                + ", ".join([f"{k} = :{k}" for k in item if k not in key_columns]),
                ExpressionAttributeValues={
                    f":{k}": v for k, v in item.items() if k not in key_columns
                },
            )


def log_error(file_key, message):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_key = f"logs/dynamodb/{file_key.split('/')[-1]}_{timestamp}_error.log"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8"))


def log_success(file_key, message):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_key = f"logs/dynamodb/{file_key.split('/')[-1]}_{timestamp}_success.log"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8"))


if __name__ == "__main__":
    event_string = os.environ.get("EVENT_DATA", "{}")
    event = json.loads(event_string)
    file_key = event.get("detail", {}).get("object", {}).get("key", "")
    if file_key.startswith("input/"):
        write_dynamodb(file_key)
