import logging
import os
import sys
import json
import pandas as pd
import boto3

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

REQUIRED_COLUMNS = {
    "products": [
        "id",
        "cost",
        "category",
        "name",
        "brand",
        "retail_price",
        "department",
    ],
    "orders": ["order_id", "user_id", "status", "created_at", "num_of_item"],
    "order_items": [
        "id",
        "order_id",
        "user_id",
        "product_id",
        "status",
        "created_at",
        "sale_price",
    ],
}


def validate_batch(files_to_validate, execution_id, result_bucket):
    logging.info(
        f"Starting validation for batch {execution_id} with {len(files_to_validate)} files."
    )

    valid_files, invalid_files = [], []
    s3_client = boto3.client("s3")

    for file_info in files_to_validate:
        bucket, key = file_info["bucket"], file_info["key"]
        file_name = os.path.basename(key)

        try:
            logging.info(f"Validating {key}...")
            file_type = None
            if "products" in file_name:
                file_type = "products"
            elif "order_items" in file_name:
                file_type = "order_items"
            elif "orders" in file_name:
                file_type = "orders"
            if not file_type:
                raise ValueError("Could not determine file type.")

            response = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(response.get("Body"))

            actual_columns = {col.strip() for col in df.columns}
            expected_columns = set(REQUIRED_COLUMNS[file_type])

            if not expected_columns.issubset(actual_columns):
                raise ValueError(
                    f"Missing columns: {list(expected_columns - actual_columns)}"
                )

            valid_files.append(file_info)
            logging.info(f"{key} is valid.")

        except Exception as e:
            logging.error(f"Validation failed for {key}: {e}")
            invalid_files.append(file_info)

    # *** Write the result to an S3 file ***
    output = {"valid_files": valid_files, "invalid_files": invalid_files}
    result_key = f"results/{execution_id}.json"

    try:
        s3_client.put_object(
            Bucket=result_bucket, Key=result_key, Body=json.dumps(output)
        )
        logging.info(
            f"Successfully wrote validation result to s3://{result_bucket}/{result_key}"
        )
    except Exception as e:
        logging.error(f"Failed to write result to S3: {e}")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    files_json = os.environ.get("FILES_JSON")
    exec_id = os.environ.get("EXECUTION_ID")
    res_bucket = os.environ.get("RESULT_BUCKET")

    if not all([files_json, exec_id, res_bucket]):
        logging.error(
            "Missing required environment variables: FILES_JSON, EXECUTION_ID, RESULT_BUCKET"
        )
        sys.exit(1)

    try:
        files = json.loads(files_json)
        validate_batch(files, exec_id, res_bucket)
    except json.JSONDecodeError:
        logging.error("Invalid JSON in FILES_JSON.")
        sys.exit(1)
