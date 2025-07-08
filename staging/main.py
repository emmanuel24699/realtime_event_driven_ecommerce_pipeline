import logging
import os
import sys
import json
import pandas as pd
import boto3
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from deltalake.exceptions import TableNotFoundError

# Configure structured logging with timestamp, log level, and message
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def stage_batch(valid_files):
    """
    Processes a batch of valid CSV files, merging each into its respective Delta table in S3.

    Args:
        valid_files (list): List of dictionaries containing bucket and key for each valid file.
    """
    logging.info(f"Starting to stage a batch of {len(valid_files)} files.")

    # Initialize S3 client and set storage options for Delta Lake
    s3_client = boto3.client("s3")
    storage_options = {"AWS_REGION": os.environ.get("AWS_REGION", "us-east-1")}

    # Process each file in the batch
    for file_info in valid_files:
        bucket = file_info["bucket"]
        key = file_info["key"]
        file_name = os.path.basename(key)

        # Determine table name, merge key, and partition columns based on file name
        table_name, merge_key, partition_cols = None, None, None
        if "products" in file_name:
            table_name, merge_key, partition_cols = "products", "id", ["department"]
        elif "order_items" in file_name:
            table_name, merge_key, partition_cols = (
                "order_items",
                "id",
                ["year", "month"],
            )
        elif "orders" in file_name:
            table_name, merge_key, partition_cols = (
                "orders",
                "order_id",
                ["year", "month"],
            )

        # Skip file if table type cannot be determined
        if not table_name:
            logging.warning(f"Could not determine table for {key}. Skipping.")
            continue

        # Define the S3 path for the Delta table
        staging_path = f"s3://{bucket}/staging/{table_name}"
        logging.info(f"Processing {key} into {staging_path}...")

        try:
            # Read the CSV file from S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            source_df = pd.read_csv(response.get("Body"))

            # Add year and month columns for partitioning if created_at exists
            if "created_at" in source_df.columns:
                dt_col = pd.to_datetime(source_df["created_at"])
                source_df["year"] = dt_col.dt.year
                source_df["month"] = dt_col.dt.month

            try:
                # Attempt to merge with existing Delta table
                delta_table = DeltaTable(staging_path, storage_options=storage_options)
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
                logging.info(f"Successfully merged {key}.")
            except TableNotFoundError:
                # If table doesn't exist, create a new Delta table
                logging.warning(
                    f"Table {staging_path} not found. Performing initial write for {key}."
                )
                write_deltalake(
                    staging_path,
                    source_df,
                    mode="overwrite",
                    partition_by=partition_cols,
                    storage_options=storage_options,
                )
                logging.info(f"Successfully created table with {key}.")

        except Exception as e:
            # Log any errors during processing and continue with the next file
            logging.error(f"Failed to stage {key}: {e}", exc_info=True)
            continue

    # Log completion of the batch staging process
    logging.info("Staging batch complete.")
    sys.exit(0)


if __name__ == "__main__":
    # Retrieve the list of files to process from environment variable
    files_json = os.environ.get("FILES_JSON")
    if not files_json:
        # Log error and exit if FILES_JSON is missing
        logging.error("FILES_JSON environment variable is required.")
        sys.exit(1)

    try:
        # Parse JSON input and start staging process
        files_to_process = json.loads(files_json)
        stage_batch(files_to_process)
    except json.JSONDecodeError:
        # Log error and exit if JSON parsing fails
        logging.error("Invalid JSON provided in FILES_JSON.")
        sys.exit(1)
