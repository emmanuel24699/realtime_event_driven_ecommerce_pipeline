import logging
import os
import sys
import json
import pandas as pd
import boto3

# Configure logging to display timestamp, log level, and message
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define required columns for each file type
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
    """
    Validates a batch of CSV files stored in S3 for required columns and logs results.

    Args:
        files_to_validate (list): List of dictionaries containing bucket and key for each file.
        execution_id (str): Unique identifier for the batch execution.
        result_bucket (str): S3 bucket to store the validation results.
    """
    # Initialize lists to track valid and invalid files
    logging.info(
        f"Starting validation for batch {execution_id} with {len(files_to_validate)} files."
    )
    valid_files, invalid_files = [], []
    s3_client = boto3.client("s3")

    # Iterate through each file to validate
    for file_info in files_to_validate:
        bucket, key = file_info["bucket"], file_info["key"]
        file_name = os.path.basename(key)

        try:
            # Log the start of validation for the current file
            logging.info(f"Validating {key}...")

            # Determine file type based on file name
            file_type = None
            if "products" in file_name:
                file_type = "products"
            elif "order_items" in file_name:
                file_type = "order_items"
            elif "orders" in file_name:
                file_type = "orders"
            if not file_type:
                raise ValueError("Could not determine file type.")

            # Read the CSV file from S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(response.get("Body"))

            # Check if all required columns are present
            actual_columns = {col.strip() for col in df.columns}
            expected_columns = set(REQUIRED_COLUMNS[file_type])

            if not expected_columns.issubset(actual_columns):
                raise ValueError(
                    f"Missing columns: {list(expected_columns - actual_columns)}"
                )

            # If validation passes, add to valid files
            valid_files.append(file_info)
            logging.info(f"{key} is valid.")

        except Exception as e:
            # Log and collect invalid files
            logging.error(f"Validation failed for {key}: {e}")
            invalid_files.append(file_info)

    # Prepare the output dictionary with validation results
    output = {"valid_files": valid_files, "invalid_files": invalid_files}
    result_key = f"results/{execution_id}.json"

    try:
        # Write the validation results to S3
        s3_client.put_object(
            Bucket=result_bucket, Key=result_key, Body=json.dumps(output)
        )
        logging.info(
            f"Successfully wrote validation result to s3://{result_bucket}/{result_key}"
        )
    except Exception as e:
        # Log failure to write results and exit with error
        logging.error(f"Failed to write result to S3: {e}")
        sys.exit(1)

    # Exit successfully if all operations complete
    sys.exit(0)


if __name__ == "__main__":
    # Retrieve environment variables
    files_json = os.environ.get("FILES_JSON")
    exec_id = os.environ.get("EXECUTION_ID")
    res_bucket = os.environ.get("RESULT_BUCKET")

    # Check if all required environment variables are present
    if not all([files_json, exec_id, res_bucket]):
        logging.error(
            "Missing required environment variables: FILES_JSON, EXECUTION_ID, RESULT_BUCKET"
        )
        sys.exit(1)

    try:
        # Parse JSON input and start validation
        files = json.loads(files_json)
        validate_batch(files, exec_id, res_bucket)
    except json.JSONDecodeError:
        # Log JSON parsing error and exit
        logging.error("Invalid JSON in FILES_JSON.")
        sys.exit(1)
