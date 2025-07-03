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


def transform_file(file_key):
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

    # Read CSV and convert to Spark DataFrame
    df = pd.read_csv(local_file)
    spark_df = spark.createDataFrame(df).withColumn(
        "order_date", col("created_at").cast("date")
    )

    # Merge into Delta Lake fact table
    delta_path = f"s3a://{BUCKET_NAME}/staging/fact/{file_type}"
    if file_type == "products":
        key = "id"
    else:
        key = "order_id" if file_type == "orders" else "id"
        spark_df.write.format("delta").partitionBy("order_date").mode("overwrite").save(
            delta_path
        )

    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.alias("target").merge(
        spark_df.alias("source"), f"target.{key} = source.{key}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # Compute KPIs (only for order_items)
    if file_type == "order_items":
        # Load fact tables
        orders_df = DeltaTable.forPath(
            spark, f"s3a://{BUCKET_NAME}/staging/fact/orders"
        ).toDF()
        products_df = DeltaTable.forPath(
            spark, f"s3a://{BUCKET_NAME}/staging/fact/products"
        ).toDF()

        # Join tables
        joined_df = spark_df.join(
            orders_df, spark_df.order_id == orders_df.order_id, "inner"
        ).join(products_df, spark_df.product_id == products_df.id, "inner")

        # Category-Level KPIs
        category_kpis = joined_df.groupBy("category", "order_date").agg(
            sum_("sale_price").alias("daily_revenue"),
            avg(
                joined_df.groupBy("category", "order_date", "order_id").sum(
                    "sale_price"
                )
            ).alias("avg_order_value"),
            (
                countDistinct(when(col("status") == "returned", col("order_id")))
                / countDistinct("order_id")
            ).alias("avg_return_rate"),
        )

        # Order-Level KPIs
        order_kpis = joined_df.groupBy("order_date").agg(
            countDistinct("order_id").alias("total_orders"),
            sum_("sale_price").alias("total_revenue"),
            sum_("num_of_item").alias("total_items_sold"),
            (
                countDistinct(when(col("status") == "returned", col("order_id")))
                / countDistinct("order_id")
            ).alias("return_rate"),
            countDistinct("user_id").alias("unique_customers"),
        )

        # Write to DynamoDB
        write_to_dynamodb(category_kpis, "CategoryKPIs", ["category", "order_date"])
        write_to_dynamodb(order_kpis, "OrderKPIs", ["order_date"])

    log_success(file_key, "Transformation successful")


def write_to_dynamodb(df, table_name, key_columns):
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


def log_success(file_key, message):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_key = f"logs/transform/{file_key.split('/')[-1]}_{timestamp}_success.log"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=log_key, Body=message.encode("utf-8"))


if __name__ == "__main__":
    event = json.loads(os.environ.get("EVENT_DATA", "{}"))
    file_key = (
        event.get("Records", [{}])[0].get("s3", {}).get("object", {}).get("key", "")
    )
    if file_key.startswith("input/"):
        transform_file(file_key)
