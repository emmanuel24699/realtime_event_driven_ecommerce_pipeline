import os
import sys
import pandas as pd
import boto3

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


def validate_file(bucket_name, file_key):
    """Validates the structure of a given file in S3."""
    print(f"Starting validation for s3://{bucket_name}/{file_key}...")

    # Determine file type from its name
    file_name = os.path.basename(file_key)
    file_type = None
    if "products" in file_name:
        file_type = "products"
    elif "order_items" in file_name:
        file_type = "order_items"
    elif "orders" in file_name:
        file_type = "orders"

    if not file_type:
        print(f"Error: Could not determine file type for '{file_name}'.")
        sys.exit(1)  # Exit with failure

    print(f"Determined file type: {file_type}")

    try:
        # Read CSV from S3
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(response.get("Body"))

        # Validate columns
        actual_columns = {col.strip() for col in df.columns}
        expected_columns = set(REQUIRED_COLUMNS[file_type])

        if not expected_columns.issubset(actual_columns):
            missing = expected_columns - actual_columns
            print(f"Error: File is missing required columns: {list(missing)}")
            sys.exit(1)  # Exit with failure

        print(f"Validation successful for s3://{bucket_name}/{file_key}.")
        sys.exit(0)  # Exit with success

    except Exception as e:
        print(f"An unexpected error occurred during validation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_key = os.environ.get("S3_KEY")

    if not s3_bucket or not s3_key:
        print("Error: S3_BUCKET and S3_KEY environment variables are required.")
        sys.exit(1)

    validate_file(s3_bucket, s3_key)
