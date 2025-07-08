import logging
import os
import sys
import json
import pandas as pd
import boto3
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from deltalake.exceptions import TableNotFoundError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def stage_batch(valid_files):
    """
    Processes a batch of valid files, merging each into its respective Delta table.
    """
    logging.info(f"Starting to stage a batch of {len(valid_files)} files.")
    s3_client = boto3.client("s3")
    storage_options = {"AWS_REGION": os.environ.get("AWS_REGION", "us-east-1")}

    for file_info in valid_files:
        bucket = file_info["bucket"]
        key = file_info["key"]
        file_name = os.path.basename(key)

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

        if not table_name:
            logging.warning(f"Could not determine table for {key}. Skipping.")
            continue

        staging_path = f"s3://{bucket}/staging/{table_name}"
        logging.info(f"Processing {key} into {staging_path}...")

        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            source_df = pd.read_csv(response.get("Body"))

            if "created_at" in source_df.columns:
                dt_col = pd.to_datetime(source_df["created_at"])
                source_df["year"] = dt_col.dt.year
                source_df["month"] = dt_col.dt.month

            try:
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
            logging.error(f"Failed to stage {key}: {e}", exc_info=True)
            continue

    logging.info("Staging batch complete.")
    sys.exit(0)


if __name__ == "__main__":
    files_json = os.environ.get("FILES_JSON")
    if not files_json:
        logging.error("FILES_JSON environment variable is required.")
        sys.exit(1)

    try:
        files_to_process = json.loads(files_json)
        stage_batch(files_to_process)
    except json.JSONDecodeError:
        logging.error("Invalid JSON provided in FILES_JSON.")
        sys.exit(1)
