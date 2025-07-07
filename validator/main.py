# import logging
# import os
# import sys
# import json
# import pandas as pd
# import boto3

# logging.basicConfig(
#     level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
# )

# REQUIRED_COLUMNS = {
#     "products": [
#         "id",
#         "cost",
#         "category",
#         "name",
#         "brand",
#         "retail_price",
#         "department",
#     ],
#     "orders": ["order_id", "user_id", "status", "created_at", "num_of_item"],
#     "order_items": [
#         "id",
#         "order_id",
#         "user_id",
#         "product_id",
#         "status",
#         "created_at",
#         "sale_price",
#     ],
# }


# def validate_batch(files_to_validate, execution_id, result_bucket):
#     logging.info(
#         f"Starting validation for batch {execution_id} with {len(files_to_validate)} files."
#     )

#     valid_files, invalid_files = [], []
#     s3_client = boto3.client("s3")

#     for file_info in files_to_validate:
#         bucket, key = file_info["bucket"], file_info["key"]
#         file_name = os.path.basename(key)

#         try:
#             logging.info(f"Validating {key}...")
#             file_type = None
#             if "products" in file_name:
#                 file_type = "products"
#             elif "order_items" in file_name:
#                 file_type = "order_items"
#             elif "orders" in file_name:
#                 file_type = "orders"
#             if not file_type:
#                 raise ValueError("Could not determine file type.")

#             response = s3_client.get_object(Bucket=bucket, Key=key)
#             df = pd.read_csv(response.get("Body"))

#             actual_columns = {col.strip() for col in df.columns}
#             expected_columns = set(REQUIRED_COLUMNS[file_type])

#             if not expected_columns.issubset(actual_columns):
#                 raise ValueError(
#                     f"Missing columns: {list(expected_columns - actual_columns)}"
#                 )

#             valid_files.append(file_info)
#             logging.info(f"{key} is valid.")

#         except Exception as e:
#             logging.error(f"Validation failed for {key}: {e}")
#             invalid_files.append(file_info)

#     # *** THE FIX: Write the result to an S3 file instead of printing ***
#     output = {"valid_files": valid_files, "invalid_files": invalid_files}
#     result_key = f"results/{execution_id}.json"

#     try:
#         s3_client.put_object(
#             Bucket=result_bucket, Key=result_key, Body=json.dumps(output)
#         )
#         logging.info(
#             f"Successfully wrote validation result to s3://{result_bucket}/{result_key}"
#         )
#     except Exception as e:
#         logging.error(f"Failed to write result to S3: {e}")
#         sys.exit(1)

#     sys.exit(0)


# if __name__ == "__main__":
#     files_json = os.environ.get("FILES_JSON")
#     exec_id = os.environ.get("EXECUTION_ID")
#     res_bucket = os.environ.get("RESULT_BUCKET")

#     if not all([files_json, exec_id, res_bucket]):
#         logging.error(
#             "Missing required environment variables: FILES_JSON, EXECUTION_ID, RESULT_BUCKET"
#         )
#         sys.exit(1)

#     try:
#         files = json.loads(files_json)
#         validate_batch(files, exec_id, res_bucket)
#     except json.JSONDecodeError:
#         logging.error("Invalid JSON in FILES_JSON.")
#         sys.exit(1)

import logging
import os
import sys
import json
import pandas as pd
import boto3
from botocore.exceptions import ClientError

# --- Enhanced Logging Setup ---
LOG_FILE = "/tmp/validator.log"

# Configure logging to write to both a local file and the console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),  # This still sends logs to CloudWatch
    ],
)


def upload_log_to_s3(log_file, bucket, execution_id):
    """Uploads the log file to a specific S3 path."""
    if os.path.exists(log_file):
        try:
            s3_client = boto3.client("s3")
            log_key = f"logs/validator/{execution_id}.log"
            s3_client.upload_file(log_file, bucket, log_key)
            logging.info(f"Successfully uploaded log file to s3://{bucket}/{log_key}")
        except ClientError as e:
            logging.error(f"Failed to upload log file to S3: {e}")


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

    output = {"valid_files": valid_files, "invalid_files": invalid_files}
    result_key = f"results/{execution_id}.json"

    s3_client.put_object(Bucket=result_bucket, Key=result_key, Body=json.dumps(output))
    logging.info(
        f"Successfully wrote validation result to s3://{result_bucket}/{result_key}"
    )
    return 0  # Return success code


if __name__ == "__main__":
    files_json = os.environ.get("FILES_JSON")
    exec_id = os.environ.get("EXECUTION_ID")
    res_bucket = os.environ.get("RESULT_BUCKET")

    if not all([files_json, exec_id, res_bucket]):
        logging.error(
            "Missing required environment variables: FILES_JSON, EXECUTION_ID, RESULT_BUCKET"
        )
        sys.exit(1)

    exit_code = 1  # Default to failure
    try:
        files = json.loads(files_json)
        exit_code = validate_batch(files, exec_id, res_bucket)
    except Exception as e:
        logging.error(
            f"An unhandled error occurred in main execution: {e}", exc_info=True
        )
    finally:
        # This block ensures logs are always uploaded
        upload_log_to_s3(LOG_FILE, res_bucket, exec_id)
        sys.exit(exit_code)
