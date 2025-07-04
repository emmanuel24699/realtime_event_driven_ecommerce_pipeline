import os
import sys
import boto3
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


def transform_and_load(bucket_name):
    """Reads from Delta tables using Spark, computes KPIs, and loads to DynamoDB."""
    print("Initializing Spark session...")

    # Initialize Spark Session with Delta Lake support
    spark = (
        SparkSession.builder.appName("Ecommerce-ETL-Transformer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    print("Spark session initialized. Starting transformation and load process...")

    try:
        # --- Read Data from Delta Lake ---
        base_path = f"s3a://{bucket_name}/staging"
        products_df = spark.read.format("delta").load(f"{base_path}/products")
        orders_df = spark.read.format("delta").load(f"{base_path}/orders")
        order_items_df = spark.read.format("delta").load(f"{base_path}/order_items")
        print("Successfully loaded data from all Delta tables into Spark.")

        # --- Data Transformation & KPI Calculation ---
        orders_df = orders_df.withColumn("order_date", F.to_date(F.col("created_at")))

        # Join the dataframes
        df = order_items_df.join(
            products_df, order_items_df.product_id == products_df.id
        ).join(orders_df, order_items_df.order_id == orders_df.order_id)

        # --- 1. Order-Level KPIs ---
        daily_kpis = df.groupBy("order_date").agg(
            F.sum("sale_price").alias("total_revenue"),
            F.count("id_item").alias("total_items_sold"),
            F.countDistinct("order_id_item").alias("total_orders"),
            F.countDistinct("user_id_item").alias("unique_customers"),
        )
        returned_orders = (
            orders_df.filter(F.col("status") == "returned")
            .groupBy("order_date")
            .agg(F.countDistinct("order_id").alias("returned_orders"))
        )

        daily_kpis_df = (
            daily_kpis.join(returned_orders, "order_date", "left_outer")
            .fillna(0)
            .withColumn(
                "return_rate", (F.col("returned_orders") / F.col("total_orders")) * 100
            )
        )

        print("Calculated Order-Level KPIs with Spark.")

        # --- 2. Category-Level KPIs ---
        category_kpis = df.groupBy("order_date", "category").agg(
            F.sum("sale_price").alias("daily_revenue"),
            F.countDistinct("order_id_item").alias("total_category_orders"),
        )
        returned_items = (
            df.filter(F.col("status_item") == "returned")
            .groupBy("order_date", "category")
            .agg(F.count("id_item").alias("returned_items"))
        )
        total_items = df.groupBy("order_date", "category").agg(
            F.count("id_item").alias("total_items")
        )

        category_kpis_df = (
            category_kpis.join(returned_items, ["order_date", "category"], "left_outer")
            .join(total_items, ["order_date", "category"], "left_outer")
            .fillna(0)
            .withColumn(
                "avg_order_value",
                F.col("daily_revenue") / F.col("total_category_orders"),
            )
            .withColumn(
                "avg_return_rate",
                (F.col("returned_items") / F.col("total_items")) * 100,
            )
        )

        print("Calculated Category-Level KPIs with Spark.")

        # --- Load to DynamoDB ---
        # Collect results to driver (KPI datasets are small) and load using Boto3
        dynamodb = boto3.resource("dynamodb")

        # Load Order-Level KPIs
        order_table = dynamodb.Table("OrderKPIs")
        with order_table.batch_writer(overwrite_by_pkeys=["order_date"]) as batch:
            for row in daily_kpis_df.collect():
                batch.put_item(
                    Item={
                        "order_date": row["order_date"].strftime("%Y-%m-%d"),
                        "total_revenue": Decimal(str(round(row["total_revenue"], 2))),
                        "total_items_sold": int(row["total_items_sold"]),
                        "total_orders": int(row["total_orders"]),
                        "unique_customers": int(row["unique_customers"]),
                        "return_rate": Decimal(str(round(row["return_rate"], 2))),
                    }
                )
        print("Loaded Order-Level KPIs to DynamoDB.")

        # Load Category-Level KPIs
        category_table = dynamodb.Table("CategoryKPIs")
        with category_table.batch_writer(
            overwrite_by_pkeys=["category", "order_date"]
        ) as batch:
            for row in category_kpis_df.collect():
                batch.put_item(
                    Item={
                        "category": row["category"],
                        "order_date": row["order_date"].strftime("%Y-%m-%d"),
                        "daily_revenue": Decimal(str(round(row["daily_revenue"], 2))),
                        "avg_order_value": Decimal(
                            str(round(row["avg_order_value"], 2))
                        ),
                        "avg_return_rate": Decimal(
                            str(round(row["avg_return_rate"], 2))
                        ),
                    }
                )
        print("Loaded Category-Level KPIs to DynamoDB.")

        spark.stop()
        print("Transformation and load process completed successfully.")
        sys.exit(0)

    except Exception as e:
        print(f"An unexpected error occurred during Spark transformation: {e}")
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    s3_bucket = os.environ.get("S3_BUCKET")
    if not s3_bucket:
        print("Error: S3_BUCKET environment variable is required.")
        sys.exit(1)
    transform_and_load(s3_bucket)
