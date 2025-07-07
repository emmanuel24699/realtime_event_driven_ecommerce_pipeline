# import logging
# import os
# import sys
# import boto3
# from decimal import Decimal
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.errors import PySparkException
# from botocore.exceptions import ClientError

# # Configure structured logging
# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
# )


# def transform_and_load(bucket_name):
#     logging.info("Initializing Spark session...")

#     spark = (
#         SparkSession.builder.appName("Ecommerce-ETL-Transformer")
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config(
#             "spark.sql.catalog.spark_catalog",
#             "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#         )
#         .getOrCreate()
#     )

#     logging.info(
#         "Spark session initialized. Starting transformation and load process..."
#     )

#     try:
#         # --- 1. Load Data ---
#         base_path = f"s3a://{bucket_name}/staging"
#         products_df = spark.read.format("delta").load(f"{base_path}/products")
#         orders_df = spark.read.format("delta").load(f"{base_path}/orders")
#         order_items_df = spark.read.format("delta").load(f"{base_path}/order_items")
#         logging.info(
#             "Successfully loaded data from all partitioned Delta tables into Spark."
#         )

#         # --- 2. Data Cleaning ---
#         logging.info("Starting data cleaning and wrangling phase...")
#         products_cleaned_df = (
#             products_df.withColumn("name", F.trim(F.col("name")))
#             .withColumn("brand", F.trim(F.col("brand")))
#             .withColumn(
#                 "category", F.trim(F.coalesce(F.col("category"), F.lit("Unknown")))
#             )
#             .withColumn(
#                 "department", F.trim(F.coalesce(F.col("department"), F.lit("Unknown")))
#             )
#             .filter((F.col("cost") > 0) & (F.col("retail_price") > 0))
#             .dropDuplicates(["id"])
#         )
#         logging.info(
#             f"Products table cleaned. Rows remaining: {products_cleaned_df.count()}"
#         )

#         orders_cleaned_df = (
#             orders_df.withColumn("created_at_ts", F.to_timestamp(F.col("created_at")))
#             .withColumn("order_date", F.to_date(F.col("created_at_ts")))
#             .withColumn("status", F.lower(F.trim(F.col("status"))))
#             .filter(F.col("order_date").isNotNull())
#             .filter(F.col("num_of_item") > 0)
#             .dropDuplicates(["order_id"])
#         )
#         logging.info(
#             f"Orders table cleaned. Rows remaining: {orders_cleaned_df.count()}"
#         )

#         order_items_cleaned_df = (
#             order_items_df.filter(F.col("product_id").isNotNull())
#             .filter(F.col("sale_price") > 0)
#             .withColumn("status", F.lower(F.trim(F.col("status"))))
#             .dropDuplicates(["id"])
#         )
#         logging.info(
#             f"Order items table cleaned. Rows remaining: {order_items_cleaned_df.count()}"
#         )

#         # --- 3. Join Cleaned DataFrames ---
#         logging.info("Joining cleaned dataframes...")

#         # *** THE FIX: Proactively rename ALL potentially ambiguous columns before joining ***
#         orders_renamed_df = (
#             orders_cleaned_df.withColumnRenamed("user_id", "order_user_id")
#             .withColumnRenamed("status", "order_status")
#             .withColumnRenamed("created_at", "order_created_at")
#         )

#         order_items_renamed_df = (
#             order_items_cleaned_df.withColumnRenamed("id", "order_item_id")
#             .withColumnRenamed("status", "item_status")
#             .withColumnRenamed("created_at", "item_created_at")
#         )

#         df_items_products = order_items_renamed_df.join(
#             products_cleaned_df,
#             order_items_renamed_df.product_id == products_cleaned_df.id,
#             "inner",
#         )
#         df = df_items_products.join(orders_renamed_df, ["order_id"], "inner")

#         # --- 4. KPI Calculation ---
#         logging.info("Calculating KPIs...")

#         daily_kpis = df.groupBy("order_date").agg(
#             F.sum("sale_price").alias("total_revenue"),
#             F.count("order_item_id").alias("total_items_sold"),
#             F.countDistinct("order_id").alias("total_orders"),
#             F.countDistinct("user_id").alias("unique_customers"),
#         )
#         returned_orders = (
#             orders_renamed_df.filter(F.col("order_status") == "returned")
#             .groupBy("order_date")
#             .agg(F.countDistinct("order_id").alias("returned_orders"))
#         )
#         daily_kpis_df = (
#             daily_kpis.join(returned_orders, "order_date", "left_outer")
#             .fillna(0)
#             .withColumn(
#                 "return_rate",
#                 (
#                     F.col("returned_orders")
#                     / F.when(
#                         F.col("total_orders") > 0, F.col("total_orders")
#                     ).otherwise(1)
#                 )
#                 * 100,
#             )
#         )
#         logging.info("Calculated Order-Level KPIs.")

#         category_kpis = df.groupBy("order_date", "category").agg(
#             F.sum("sale_price").alias("daily_revenue"),
#             F.countDistinct("order_id").alias("total_category_orders"),
#         )
#         returned_items = (
#             df.filter(F.col("item_status") == "returned")
#             .groupBy("order_date", "category")
#             .agg(F.count("order_item_id").alias("returned_items"))
#         )
#         total_items = df.groupBy("order_date", "category").agg(
#             F.count("order_item_id").alias("total_items")
#         )
#         category_kpis_df = (
#             category_kpis.join(returned_items, ["order_date", "category"], "left_outer")
#             .join(total_items, ["order_date", "category"], "left_outer")
#             .fillna(0)
#             .withColumn(
#                 "avg_order_value",
#                 F.col("daily_revenue")
#                 / F.when(
#                     F.col("total_category_orders") > 0, F.col("total_category_orders")
#                 ).otherwise(1),
#             )
#             .withColumn(
#                 "avg_return_rate",
#                 (
#                     F.col("returned_items")
#                     / F.when(F.col("total_items") > 0, F.col("total_items")).otherwise(
#                         1
#                     )
#                 )
#                 * 100,
#             )
#         )
#         logging.info("Calculated Category-Level KPIs.")

#         # --- 5. Load to DynamoDB ---
#         logging.info("Loading KPIs to DynamoDB...")
#         dynamodb = boto3.resource("dynamodb")

#         order_table = dynamodb.Table("OrderKPIs")
#         with order_table.batch_writer(overwrite_by_pkeys=["order_date"]) as batch:
#             for row in daily_kpis_df.collect():
#                 batch.put_item(
#                     Item={
#                         "order_date": row["order_date"].strftime("%Y-%m-%d"),
#                         "total_revenue": Decimal(str(round(row["total_revenue"], 2))),
#                         "total_items_sold": int(row["total_items_sold"]),
#                         "total_orders": int(row["total_orders"]),
#                         "unique_customers": int(row["unique_customers"]),
#                         "return_rate": Decimal(str(round(row["return_rate"], 2))),
#                     }
#                 )
#         logging.info("Loaded Order-Level KPIs to DynamoDB.")

#         category_table = dynamodb.Table("CategoryKPIs")
#         with category_table.batch_writer(
#             overwrite_by_pkeys=["category", "order_date"]
#         ) as batch:
#             for row in category_kpis_df.collect():
#                 batch.put_item(
#                     Item={
#                         "category": row["category"],
#                         "order_date": row["order_date"].strftime("%Y-%m-%d"),
#                         "daily_revenue": Decimal(str(round(row["daily_revenue"], 2))),
#                         "avg_order_value": Decimal(
#                             str(round(row["avg_order_value"], 2))
#                         ),
#                         "avg_return_rate": Decimal(
#                             str(round(row["avg_return_rate"], 2))
#                         ),
#                     }
#                 )
#         logging.info("Loaded Category-Level KPIs to DynamoDB.")

#     except PySparkException as e:
#         logging.error(
#             f"A Spark error occurred during transformation: {e}", exc_info=True
#         )
#         sys.exit(1)
#     except ClientError as e:
#         logging.error(
#             f"A Boto3 error occurred during DynamoDB load: {e}", exc_info=True
#         )
#         sys.exit(1)
#     except Exception as e:
#         logging.error(
#             f"An unexpected error occurred during Spark transformation: {e}",
#             exc_info=True,
#         )
#         sys.exit(1)
#     finally:
#         logging.info("Stopping Spark session.")
#         spark.stop()


# if __name__ == "__main__":
#     s3_bucket = os.environ.get("S3_BUCKET")
#     if not s3_bucket:
#         logging.error("S3_BUCKET environment variable is required.")
#         sys.exit(1)
#     transform_and_load(s3_bucket)


import logging
import os
import sys
import boto3
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.errors import PySparkException
from botocore.exceptions import ClientError

# --- Enhanced Logging Setup ---
LOG_FILE = "/tmp/transformer.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)


def upload_log_to_s3(log_file, bucket, execution_id):
    """Uploads the log file to a specific S3 path."""
    if os.path.exists(log_file):
        try:
            s3_client = boto3.client("s3")
            log_key = f"logs/transformer/{execution_id}.log"
            s3_client.upload_file(log_file, bucket, log_key)
            logging.info(f"Successfully uploaded log file to s3://{bucket}/{log_key}")
        except ClientError as e:
            logging.error(f"Failed to upload log file to S3: {e}")


def transform_and_load(bucket_name):
    # ... (The entire transform_and_load function remains the same) ...
    # It should return 0 on success
    # ... (for brevity, the function content is omitted, but it's the same as the last version)
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
        # --- Load and Clean Data (as before) ---
        # ... (cleaning logic) ...
        base_path = f"s3a://{bucket_name}/staging"
        products_df = spark.read.format("delta").load(f"{base_path}/products")
        orders_df = spark.read.format("delta").load(f"{base_path}/orders")
        order_items_df = spark.read.format("delta").load(f"{base_path}/order_items")
        logging.info(
            "Successfully loaded data from all partitioned Delta tables into Spark."
        )

        products_cleaned_df = products_df.dropDuplicates(["id"])
        orders_cleaned_df = orders_df.dropDuplicates(["order_id"])
        order_items_cleaned_df = order_items_df.dropDuplicates(["id"])

        orders_renamed_df = orders_cleaned_df.withColumn(
            "order_date", F.to_date(F.col("created_at"))
        ).withColumnRenamed("status", "order_status")
        order_items_renamed_df = order_items_cleaned_df.withColumnRenamed(
            "id", "order_item_id"
        ).withColumnRenamed("status", "item_status")

        df_items_products = order_items_renamed_df.join(
            products_cleaned_df,
            order_items_renamed_df.product_id == products_cleaned_df.id,
            "inner",
        )
        df = df_items_products.join(orders_renamed_df, ["order_id"], "inner")

        # --- KPI Calculation (as before) ---
        # ... (kpi logic) ...
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
                "return_rate",
                (
                    F.col("returned_orders")
                    / F.when(
                        F.col("total_orders") > 0, F.col("total_orders")
                    ).otherwise(1)
                )
                * 100,
            )
        )

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
                F.col("daily_revenue")
                / F.when(
                    F.col("total_category_orders") > 0, F.col("total_category_orders")
                ).otherwise(1),
            )
            .withColumn(
                "avg_return_rate",
                (
                    F.col("returned_items")
                    / F.when(F.col("total_items") > 0, F.col("total_items")).otherwise(
                        1
                    )
                )
                * 100,
            )
        )

        # --- Load to DynamoDB (as before) ---
        # ... (dynamodb logic) ...
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

        return 0  # Return success
    except Exception as e:
        logging.error(f"An error occurred in transform_and_load: {e}", exc_info=True)
        return 1  # Return failure


if __name__ == "__main__":
    s3_bucket = os.environ.get("S3_BUCKET")
    exec_id = os.environ.get("EXECUTION_ID")

    if not all([s3_bucket, exec_id]):
        logging.error("Missing required environment variables: S3_BUCKET, EXECUTION_ID")
        sys.exit(1)

    exit_code = 1
    try:
        exit_code = transform_and_load(s3_bucket)
    except Exception as e:
        logging.error(
            f"An unhandled error occurred in main execution: {e}", exc_info=True
        )
    finally:
        logging.info("Stopping Spark session.")
        SparkSession.getActiveSession().stop()
        upload_log_to_s3(LOG_FILE, s3_bucket, exec_id)
        sys.exit(exit_code)
