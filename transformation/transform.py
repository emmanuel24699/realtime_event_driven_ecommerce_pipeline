import pandas as pd
import boto3
from datetime import datetime
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import json
import os
from pyspark.sql.functions import col, sum as _sum, count, avg, countDistinct, when
from decimal import Decimal
import logging
import sys

# --- 1. Configure Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

# --- 2. Initialize Clients and Constants ---
try:
    s3_client = boto3.client("s3", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    BUCKET_NAME = "lab6-realtime-ecommerce-pipelines"

    spark = (
        SparkSession.builder.appName("EcommerceKPITransformation")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

except Exception as e:
    logging.critical(
        f"Failed to initialize Spark or AWS clients. Shutting down. Error: {e}"
    )
    sys.exit(1)


def transform_file(file_key):
    """
    Transforms a single raw data file from S3, updates a Delta Lake fact table,
    and computes KPIs for DynamoDB.
    """
    file_name = file_key.split("/")[-1]
    local_file = f"/tmp/{file_name}"

    try:
        # --- 4. Download and Read Data ---
        logging.info(f"Downloading {file_key} from S3 to {local_file}...")
        s3_client.download_file(BUCKET_NAME, file_key, local_file)

        logging.info("Reading CSV and creating Spark DataFrame...")
        pandas_df = pd.read_csv(local_file)
        if pandas_df.empty:
            logging.warning(f"Source file {file_name} is empty. No data to process.")
            return

        spark_df = spark.createDataFrame(pandas_df)
        logging.info(f"Successfully created DataFrame with {spark_df.count()} rows.")

        if "created_at" in spark_df.columns:
            spark_df = spark_df.withColumn("order_date", col("created_at").cast("date"))

        # --- 5. Write to Delta Lake (Fact Table) ---
        if "products" in file_name:
            file_type, key = "products", "id"
        elif "orders" in file_name:
            file_type, key = "orders", "order_id"
        elif "order_items" in file_name:
            file_type, key = "order_items", "id"
        else:
            logging.error(f"Unknown file type for {file_name}. Skipping.")
            return

        delta_path = f"s3a://{BUCKET_NAME}/staging/fact/{file_type}"
        logging.info(f"Preparing to write to Delta table: {delta_path}")

        if not DeltaTable.isDeltaTable(spark, delta_path):
            logging.info(
                f"Delta table does not exist. Creating and writing {spark_df.count()} rows."
            )
            writer = spark_df.write.format("delta").mode("overwrite")
            if "order_date" in spark_df.columns:
                writer = writer.partitionBy("order_date")
            writer.save(delta_path)
        else:
            logging.info(f"Delta table exists. Merging {spark_df.count()} rows...")
            delta_table = DeltaTable.forPath(spark, delta_path)
            delta_table.alias("target").merge(
                spark_df.alias("source"), f"target.{key} = source.{key}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logging.info(f"Successfully wrote data to {delta_path}.")

        # --- 6. Calculate and Write KPIs (Only for order_items) ---
        if file_type == "order_items":
            calculate_and_write_kpis(spark_df)

        log_success(file_key, "Transformation successful")

    except Exception as e:
        logging.error(
            f"An error occurred during transformation of {file_key}. Error: {e}",
            exc_info=True,
        )
        raise


def calculate_and_write_kpis(order_items_df):
    """
    Loads dimension tables, joins data, and calculates KPIs.
    """
    try:
        logging.info("Calculating KPIs for order_items...")
        orders_df = spark.read.format("delta").load(
            f"s3a://{BUCKET_NAME}/staging/fact/orders"
        )
        products_df = spark.read.format("delta").load(
            f"s3a://{BUCKET_NAME}/staging/fact/products"
        )

        if orders_df.rdd.isEmpty() or products_df.rdd.isEmpty():
            logging.warning(
                "Cannot calculate KPIs because 'orders' or 'products' Delta table is empty. Please process them first."
            )
            return

        joined_df = (
            order_items_df.alias("oi")
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

        joined_count = joined_df.count()
        if joined_count == 0:
            logging.warning(
                "Join operation resulted in 0 rows. No KPIs will be generated. Check for matching keys."
            )
            return

        logging.info(f"Join successful. Processing {joined_count} rows for KPIs.")

        revenue_per_order = joined_df.groupBy("order_date", "category", "order_id").agg(
            _sum("sale_price").alias("order_revenue")
        )
        category_kpis = revenue_per_order.groupBy("order_date", "category").agg(
            _sum("order_revenue").alias("daily_revenue"),
            avg("order_revenue").alias("avg_order_value"),
        )

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

        write_to_dynamodb(category_kpis, "CategoryKPIs", ["category", "order_date"])
        write_to_dynamodb(order_kpis, "OrderKPIs", ["order_date"])

    except Exception as e:
        logging.error(f"Failed to calculate or write KPIs. Error: {e}", exc_info=True)
        raise


def write_to_dynamodb(df, table_name, key_columns):
    """Writes a Spark DataFrame to a DynamoDB table using a robust upsert."""
    try:
        logging.info(f"Writing {df.count()} records to DynamoDB table: {table_name}")
        table = dynamodb.Table(table_name)
        for row in df.collect():
            item = row.asDict(recursive=True)
            final_item = {
                k: (
                    Decimal(str(v))
                    if isinstance(v, float)
                    else v.strftime("%Y-%m-%d") if isinstance(v, datetime) else v
                )
                for k, v in item.items()
                if v is not None
            }
            key = {k: final_item[k] for k in key_columns}

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
        logging.info(f"Successfully wrote records to {table_name}.")
    except Exception as e:
        logging.error(
            f"Failed to write to DynamoDB table {table_name}. Error: {e}", exc_info=True
        )
        raise


def log_success(file_key, message):
    """Logs a success message to S3 for audit purposes."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = file_key.split("/")[-1]
        log_key = f"logs/transform/{file_name}_{timestamp}_success.log"
        s3_client.put_object(
            Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8")
        )
    except Exception as e:
        logging.warning(f"Could not write success log to S3. Error: {e}")


if __name__ == "__main__":
    logging.info("--- Starting Transformation Script ---")
    event_str = os.environ.get("EVENT_DATA", "{}")

    try:
        event = json.loads(event_str)
        file_key = event.get("detail", {}).get("object", {}).get("key", "")

        if file_key and file_key.startswith("input/"):
            transform_file(file_key)
        elif not file_key:
            logging.error("File key not found in the event data.")
        else:
            logging.warning(
                f"File key '{file_key}' is not in the 'input/' directory. Skipping."
            )

    except json.JSONDecodeError:
        logging.error(f"Error decoding EVENT_DATA environment variable: {event_str}")
    except Exception as e:
        logging.critical(
            f"A critical unexpected error occurred in the main execution block. Error: {e}",
            exc_info=True,
        )

    logging.info("--- Transformation Script Finished ---")
