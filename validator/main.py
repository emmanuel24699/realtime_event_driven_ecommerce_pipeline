# import logging
# import os
# import sys
# import pandas as pd
# import boto3
# from botocore.exceptions import ClientError

# # Configure structured logging
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


# def validate_file(bucket_name, file_key):
#     logging.info(f"Starting validation for s3://{bucket_name}/{file_key}...")

#     file_name = os.path.basename(file_key)
#     file_type = None
#     if "products" in file_name:
#         file_type = "products"
#     elif "order_items" in file_name:
#         file_type = "order_items"
#     elif "orders" in file_name:
#         file_type = "orders"

#     if not file_type:
#         logging.error(f"Could not determine file type for '{file_name}'. Skipping.")
#         sys.exit(1)

#     logging.info(f"Determined file type: {file_type}")

#     try:
#         s3_client = boto3.client("s3")
#         response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
#         df = pd.read_csv(response.get("Body"))

#         actual_columns = {col.strip() for col in df.columns}
#         expected_columns = set(REQUIRED_COLUMNS[file_type])

#         if not expected_columns.issubset(actual_columns):
#             missing = expected_columns - actual_columns
#             logging.error(f"File is missing required columns: {list(missing)}")
#             sys.exit(1)

#         logging.info(
#             f"Validation successful for s3://{bucket_name}/{file_key}. All required columns are present."
#         )
#         sys.exit(0)

#     except ClientError as e:
#         logging.error(f"AWS Boto3 client error during validation: {e}")
#         sys.exit(1)
#     except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
#         logging.error(f"Pandas parsing error for file {file_key}: {e}")
#         sys.exit(1)
#     except Exception as e:
#         logging.error(
#             f"An unexpected error occurred during validation: {e}", exc_info=True
#         )
#         sys.exit(1)


# if __name__ == "__main__":
#     s3_bucket = os.environ.get("S3_BUCKET")
#     s3_key = os.environ.get("S3_KEY")

#     if not s3_bucket or not s3_key:
#         logging.error("S3_BUCKET and S3_KEY environment variables are required.")
#         sys.exit(1)

#     validate_file(s3_bucket, s3_key)


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

    # *** THE FIX: Write the result to an S3 file instead of printing ***
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
