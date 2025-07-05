import boto3
import json
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
result_bucket = os.environ.get("RESULT_BUCKET")

if not result_bucket:
    raise ValueError("Missing required environment variable: RESULT_BUCKET")


def lambda_handler(event, context):
    """
    Reads the JSON result file from S3 written by the validator task
    and merges its content back into the event payload.
    """
    execution_id = event.get("execution_id")
    if not execution_id:
        raise ValueError("Input must contain an 'execution_id'")

    result_key = f"results/{execution_id}.json"
    logger.info(f"Reading validation result from s3://{result_bucket}/{result_key}")

    try:
        response = s3.get_object(Bucket=result_bucket, Key=result_key)
        content = response["Body"].read().decode("utf-8")
        result_data = json.loads(content)

        # Merge the result back into the original input payload
        event["ValidationResult"] = {"Payload": result_data}

        return event

    except Exception as e:
        logger.error(f"Failed to read or parse result file: {e}", exc_info=True)
        raise e
