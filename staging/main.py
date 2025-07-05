import logging
import os
import sys
import pandas as pd
import boto3
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from deltalake.exceptions import TableNotFoundError
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def stage_data(bucket_name, file_key):
    logging.info(f"Starting staging/merge for s3://{bucket_name}/{file_key}...")

    file_name = os.path.basename(file_key)
    table_name = None
    merge_key = None
    partition_cols = None

    if "products" in file_name:
        table_name = "products"
        merge_key = "id"
        partition_cols = ["department"]
    elif "order_items" in file_name:
        table_name = "order_items"
        merge_key = "id"
        partition_cols = ["year", "month"]
    elif "orders" in file_name:
        table_name = "orders"
        merge_key = "order_id"
        partition_cols = ["year", "month"]

    if not table_name:
        logging.error(f"Could not determine table name for '{file_name}'.")
        sys.exit(1)

    staging_path = f"s3://{bucket_name}/staging/{table_name}"
    storage_options = {"AWS_REGION": os.environ.get("AWS_REGION", "us-east-1")}

    logging.info(
        f"Staging to table: {table_name}, Merge key: {merge_key}, Partitions: {partition_cols}"
    )

    try:
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        source_df = pd.read_csv(response.get("Body"))

        # --- Add partition columns ---
        if "created_at" in source_df.columns:
            dt_col = pd.to_datetime(source_df["created_at"])
            source_df["year"] = dt_col.dt.year
            source_df["month"] = dt_col.dt.month

        delta_table = DeltaTable(staging_path, storage_options=storage_options)

        logging.info(f"Target table found. Merging data into {staging_path}...")
        (
            delta_table.merge(
                source=source_df,
                predicate=f"target.{merge_key} = source.{merge_key}",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        logging.info(f"Successfully merged data into {staging_path}.")

    except TableNotFoundError:
        logging.warning(
            f"Table {staging_path} not found. Performing initial partitioned write."
        )
        write_deltalake(
            staging_path,
            source_df,
            mode="overwrite",
            partition_by=partition_cols,
            storage_options=storage_options,
        )
        logging.info(f"Successfully created and wrote initial data to {staging_path}.")

    except (ClientError, Exception) as e:
        logging.error(
            f"An unexpected error occurred during staging merge: {e}", exc_info=True
        )
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_key = os.environ.get("S3_KEY")

    if not s3_bucket or not s3_key:
        logging.error("S3_BUCKET and S3_KEY environment variables are required.")
        sys.exit(1)

    stage_data(s3_bucket, s3_key)
