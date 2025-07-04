import pandas as pd
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import json
import os
from pyspark.sql.functions import col, sum as _sum, count, avg, countDistinct, when
from decimal import Decimal

# Initialize AWS clients
s3_client = boto3.client("s3", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET_NAME = "lab6-realtime-ecommerce-pipelines"

# Initialize Spark
# Added spark.driver.host config to prevent UnknownHostException in containers
spark = (
    SparkSession.builder.appName("EcommerceKPITransformation")
    .config("spark.driver.host", "localhost")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)


def transform_file(file_key):
    """
    Transforms a single raw data file from S3, updates a Delta Lake fact table,
    and computes KPIs for DynamoDB.
    """
    # Determine file type from the file key
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
    pandas_df = pd.read_csv(local_file)
    spark_df = spark.createDataFrame(pandas_df)

    # Add an order_date column for partitioning
    if "created_at" in spark_df.columns:
        spark_df = spark_df.withColumn("order_date", col("created_at").cast("date"))

    # Define the key for the merge operation
    if file_type == "products":
        key = "id"
    elif file_type == "orders":
        key = "order_id"
    else:  # order_items
        key = "id"

    delta_path = f"s3a://{BUCKET_NAME}/staging/fact/{file_type}"

    # Write to Delta Lake using a merge operation for idempotency
    if not DeltaTable.isDeltaTable(spark, delta_path):
        # If the table doesn't exist, create it
        partition_columns = ["order_date"] if "order_date" in spark_df.columns else []
        writer = spark_df.write.format("delta").mode("overwrite")
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.save(delta_path)
    else:
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            spark_df.alias("source"), f"target.{key} = source.{key}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # Compute KPIs only when processing order_items
    if file_type == "order_items":
        # Load dimension tables
        orders_df = spark.read.format("delta").load(
            f"s3a://{BUCKET_NAME}/staging/fact/orders"
        )
        products_df = spark.read.format("delta").load(
            f"s3a://{BUCKET_NAME}/staging/fact/products"
        )

        # Join tables
        joined_df = (
            spark_df.alias("oi")
            .join(
                orders_df.alias("o"), col("oi.order_id") == col("o.order_id"), "inner"
            )
            .join(products_df.alias("p"), col("oi.product_id") == col("p.id"), "inner")
            .select(
                col("oi.order_id"),
                col("oi.sale_price"),
                col("o.user_id"),
                col("o.status"),
                col("o.order_date"),
                col("p.category"),
                col("oi.num_of_item"),
            )
        )

        # Pre-calculate revenue per order for AOV calculation
        revenue_per_order = joined_df.groupBy("order_date", "category", "order_id").agg(
            _sum("sale_price").alias("order_revenue")
        )

        # Category-Level KPIs
        category_kpis = revenue_per_order.groupBy("order_date", "category").agg(
            _sum("order_revenue").alias("daily_revenue"),
            avg("order_revenue").alias("avg_order_value"),
        )

        # Order-Level KPIs
        order_kpis = joined_df.groupBy("order_date").agg(
            countDistinct("order_id").alias("total_orders"),
            _sum("sale_price").alias("total_revenue"),
            _sum("num_of_item").alias("total_items_sold"),
            (
                countDistinct(when(col("status") == "Returned", col("order_id")))
                / countDistinct("order_id")
            ).alias("return_rate"),
            countDistinct("user_id").alias("unique_customers"),
        )

        # Write to DynamoDB
        write_to_dynamodb(category_kpis, "CategoryKPIs", ["category", "order_date"])
        write_to_dynamodb(order_kpis, "OrderKPIs", ["order_date"])

    log_success(file_key, "Transformation successful")


def write_to_dynamodb(df, table_name, key_columns):
    """
    Writes a Spark DataFrame to a DynamoDB table using a robust upsert.
    Handles data type conversion for DynamoDB compatibility.
    """
    table = dynamodb.Table(table_name)
    for row in df.collect():
        item = row.asDict(recursive=True)

        # Clean and convert data types for DynamoDB
        final_item = {}
        for k, v in item.items():
            if v is not None:
                if isinstance(v, float):
                    final_item[k] = Decimal(str(v))
                elif isinstance(v, datetime):
                    final_item[k] = v.strftime("%Y-%m-%d")
                else:
                    final_item[k] = v

        key = {k: final_item[k] for k in key_columns}

        # Build the update expression dynamically for an upsert operation
        update_expression_parts = []
        expression_attribute_values = {}
        expression_attribute_names = {}

        for k, v in final_item.items():
            if k not in key_columns:
                update_expression_parts.append(f"#{k} = :{k}")
                expression_attribute_values[f":{k}"] = v
                expression_attribute_names[f"#{k}"] = k

        if update_expression_parts:
            update_expression = "SET " + ", ".join(update_expression_parts)
            table.update_item(
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ExpressionAttributeNames=expression_attribute_names,
            )


def log_success(file_key, message):
    """Logs a success message to S3."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = file_key.split("/")[-1]
    log_key = f"logs/transform/{file_name}_{timestamp}_success.log"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8"))


if __name__ == "__main__":
    event_str = os.environ.get("EVENT_DATA", "{}")
    try:
        event = json.loads(event_str)
        if "Records" in event and event["Records"]:
            file_key = (
                event["Records"][0].get("s3", {}).get("object", {}).get("key", "")
            )
            if file_key and file_key.startswith("input/"):
                transform_file(file_key)
            elif not file_key:
                print("File key not found in the event.")
        else:
            print("No records found in the event, or event is empty.")
    except json.JSONDecodeError:
        print(f"Error decoding EVENT_DATA: {event_str}")
