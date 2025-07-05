import boto3
import json
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients from environment variables
sqs_queue_url = os.environ.get("SQS_QUEUE_URL")
step_function_arn = os.environ.get("STEP_FUNCTION_ARN")

if not sqs_queue_url or not step_function_arn:
    raise ValueError(
        "Missing required environment variables: SQS_QUEUE_URL and/or STEP_FUNCTION_ARN"
    )

sqs = boto3.client("sqs")
sfn = boto3.client("stepfunctions")


def lambda_handler(event, context):
    """
    This function is triggered by an EventBridge schedule. It polls an SQS queue
    for S3 event notifications, batches them, and starts a single Step Function
    execution with the list of files.
    """
    logger.info("Orchestrator started. Polling SQS queue...")

    try:
        # Poll the queue for a batch of messages
        response = sqs.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=10,  # Process up to 10 files per batch
            WaitTimeSeconds=10,  # Wait up to 10 seconds for messages
        )

        messages = response.get("Messages", [])

        if not messages:
            logger.info("No new files found in the queue. Exiting.")
            return {"statusCode": 200, "body": json.dumps("No new files to process.")}

        logger.info(f"Found {len(messages)} new files to process.")

        file_keys = []
        receipt_handles = []

        for message in messages:
            body = json.loads(message["Body"])
            # S3 event messages can contain multiple records
            for record in body.get("Records", []):
                s3_info = record.get("s3", {})
                bucket_name = s3_info.get("bucket", {}).get("name")
                object_key = s3_info.get("object", {}).get("key")

                if bucket_name and object_key:
                    file_keys.append({"bucket": bucket_name, "key": object_key})

            receipt_handles.append(message["ReceiptHandle"])

        # Prepare the input for the Step Function
        sfn_input = {"files": file_keys}

        # Start one Step Function execution with the entire batch of files
        logger.info(
            f"Starting Step Function execution with input: {json.dumps(sfn_input)}"
        )
        sfn.start_execution(
            stateMachineArn=step_function_arn, input=json.dumps(sfn_input)
        )

        # Delete the processed messages from the queue
        for handle in receipt_handles:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=handle)

        logger.info(
            f"Successfully started pipeline and deleted {len(messages)} messages from the queue."
        )
        return {
            "statusCode": 200,
            "body": json.dumps(
                f"Successfully started pipeline for {len(file_keys)} files."
            ),
        }

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        # We do not delete messages on error, so they can be retried
        raise e
