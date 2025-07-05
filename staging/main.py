import os
import sys
import pandas as pd
import boto3
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from deltalake.exceptions import TableNotFoundError  # <-- FIX #1: Import the exception


def stage_data(bucket_name, file_key):
    """
    Reads a validated file and merges (upserts) it into a Delta Lake table.
    """
    print(f"Starting staging/merge for s3://{bucket_name}/{file_key}...")

    file_name = os.path.basename(file_key)
    table_name = None
    merge_key = None

    if "products" in file_name:
        table_name = "products"
        merge_key = "id"
    elif "order_items" in file_name:
        table_name = "order_items"
        merge_key = "id"
    elif "orders" in file_name:
        table_name = "orders"
        merge_key = "order_id"

    if not table_name:
        print(f"Error: Could not determine table name for '{file_name}'.")
        sys.exit(1)

    staging_path = f"s3://{bucket_name}/staging/{table_name}"
    storage_options = {"AWS_REGION": os.environ.get("AWS_REGION", "us-east-1")}

    print(f"Staging data to Delta table: {staging_path} on merge key: {merge_key}")

    try:
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        source_df = pd.read_csv(response.get("Body"))

        for col in source_df.columns:
            if "date" in col and source_df[col].dtype == "object":
                source_df[col] = pd.to_datetime(source_df[col], errors="coerce")

        delta_table = DeltaTable(staging_path, storage_options=storage_options)

        print(f"Target table found. Merging data...")
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
        print(f"Successfully merged data into {staging_path}.")

    except TableNotFoundError:  # <-- FIX #2: Use the imported exception
        print(f"Table {staging_path} not found. Performing initial write.")
        write_deltalake(
            staging_path, source_df, mode="overwrite", storage_options=storage_options
        )
        print(f"Successfully created and wrote initial data to {staging_path}.")

    except Exception as e:
        print(f"An unexpected error occurred during staging merge: {e}")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_key = os.environ.get("S3_KEY")

    if not s3_bucket or not s3_key:
        print("Error: S3_BUCKET and S3_KEY environment variables are required.")
        sys.exit(1)

    stage_data(s3_bucket, s3_key)
