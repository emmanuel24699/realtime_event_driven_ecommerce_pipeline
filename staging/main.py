# staging/main.py
import os
import sys
import pandas as pd
import boto3
from deltalake.writer import write_deltalake

def stage_data(bucket_name, file_key):
    """
    Reads a validated file using boto3 and writes it to a Delta Lake table.
    """
    print(f"Starting staging for s3://{bucket_name}/{file_key}...")
    
    # Determine table name from file name
    file_name = os.path.basename(file_key)
    table_name = None
    if "products" in file_name:
        table_name = "products"
    elif "order_items" in file_name:
        table_name = "order_items"
    elif "orders" in file_name:
        table_name = "orders"
        
    if not table_name:
        print(f"Error: Could not determine table name for '{file_name}'.")
        sys.exit(1)

    staging_path = f"s3://{bucket_name}/staging/{table_name}"
    print(f"Staging data to Delta table at: {staging_path}")

    try:
        # ** FIX: Use boto3 to get the S3 object first **
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        
        # Read the object's body directly into pandas
        df = pd.read_csv(response.get("Body"))
        
        # Ensure date columns are handled correctly before writing
        for col in df.columns:
            if 'date' in col and df[col].dtype == 'object':
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Write to Delta Lake, appending the new data
        storage_options = {"AWS_REGION": os.environ.get("AWS_REGION", "us-east-1")}
        write_deltalake(
            staging_path,
            df,
            mode='append',
            storage_options=storage_options
        )

        print(f"Successfully staged data to {staging_path}.")
        sys.exit(0)

    except Exception as e:
        print(f"An unexpected error occurred during staging: {e}")
        sys.exit(1)

if __name__ == "__main__":
    s3_bucket = os.environ.get("S3_BUCKET")
    s3_key = os.environ.get("S3_KEY")
    
    if not s3_bucket or not s3_key:
        print("Error: S3_BUCKET and S3_KEY environment variables are required.")
        sys.exit(1)
        
    stage_data(s3_bucket, s3_key)
