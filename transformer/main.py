import logging
import os
import sys
import boto3
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.errors import PySparkException
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def transform_and_load(bucket_name):
    logging.info("Initializing Spark session...")

    spark = (
        SparkSession.builder.appName("Ecommerce-ETL-Transformer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    logging.info(
        "Spark session initialized. Starting transformation and load process..."
    )

    try:
        base_path = f"s3a://{bucket_name}/staging"
        products_df = spark.read.format("delta").load(f"{base_path}/products")
        orders_df = spark.read.format("delta").load(f"{base_path}/orders")
        order_items_df = spark.read.format("delta").load(f"{base_path}/order_items")
        logging.info(
            "Successfully loaded data from all partitioned Delta tables into Spark."
        )

        # --- Data Transformation & KPI Calculation ---
        orders_renamed_df = orders_df.withColumn(
            "order_date", F.to_date(F.col("created_at"))
        ).withColumnRenamed("status", "order_status")

        order_items_renamed_df = order_items_df.withColumnRenamed(
            "id", "order_item_id"
        ).withColumnRenamed("status", "item_status")

        # *** THE FIX: Perform an explicit join and then drop the duplicate column ***
        df_items_products = order_items_renamed_df.join(
            products_df, order_items_renamed_df.product_id == products_df.id, "inner"
        )

        # Join on the ambiguous key, which creates two 'order_id' columns
        df_with_duplicates = df_items_products.join(
            orders_renamed_df,
            df_items_products.order_id == orders_renamed_df.order_id,
            "inner",
        )

        # Immediately drop the duplicate column from one of the original dataframes
        df = df_with_duplicates.drop(orders_renamed_df.order_id)

        # Now, 'df' has a single, unambiguous 'order_id' column

        daily_kpis = df.groupBy("order_date").agg(
            F.sum("sale_price").alias("total_revenue"),
            F.count("order_item_id").alias("total_items_sold"),
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("user_id").alias("unique_customers"),
        )
        returned_orders = (
            orders_renamed_df.filter(F.col("order_status") == "returned")
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
        logging.info("Calculated Order-Level KPIs with Spark.")

        category_kpis = df.groupBy("order_date", "category").agg(
            F.sum("sale_price").alias("daily_revenue"),
            F.countDistinct("order_id").alias("total_category_orders"),
        )
        returned_items = (
            df.filter(F.col("item_status") == "returned")
            .groupBy("order_date", "category")
            .agg(F.count("order_item_id").alias("returned_items"))
        )
        total_items = df.groupBy("order_date", "category").agg(
            F.count("order_item_id").alias("total_items")
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
        logging.info("Calculated Category-Level KPIs with Spark.")

        # --- Load to DynamoDB ---
        dynamodb = boto3.resource("dynamodb")

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
        logging.info("Loaded Order-Level KPIs to DynamoDB.")

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
        logging.info("Loaded Category-Level KPIs to DynamoDB.")

    except PySparkException as e:
        logging.error(
            f"A Spark error occurred during transformation: {e}", exc_info=True
        )
        sys.exit(1)
    except ClientError as e:
        logging.error(
            f"A Boto3 error occurred during DynamoDB load: {e}", exc_info=True
        )
        sys.exit(1)
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during Spark transformation: {e}",
            exc_info=True,
        )
        sys.exit(1)
    finally:
        logging.info("Stopping Spark session.")
        spark.stop()


if __name__ == "__main__":
    s3_bucket = os.environ.get("S3_BUCKET")
    if not s3_bucket:
        logging.error("S3_BUCKET environment variable is required.")
        sys.exit(1)
    transform_and_load(s3_bucket)
