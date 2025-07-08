import boto3
import json
import os
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients using environment variables for SQS and Step Functions
sqs_queue_url = os.environ.get("SQS_QUEUE_URL")
step_function_arn = os.environ.get("STEP_FUNCTION_ARN")

# Validate that required environment variables are set
if not sqs_queue_url or not step_function_arn:
    raise ValueError(
        "Missing required environment variables: SQS_QUEUE_URL and/or STEP_FUNCTION_ARN"
    )

# Create boto3 clients for SQS and Step Functions
sqs = boto3.client("sqs")
sfn = boto3.client("stepfunctions")


def lambda_handler(event, context):
    """
    AWS Lambda function to poll an SQS queue for new file notifications and trigger
    a Step Function execution with the collected files and a unique execution ID.
    """
    logger.info("Orchestrator started. Polling SQS queue...")

    try:
        # Poll SQS queue for up to 100 messages with a 30-second wait time
        response = sqs.receive_message(
            QueueUrl=sqs_queue_url, MaxNumberOfMessages=100, WaitTimeSeconds=30
        )
        messages = response.get("Messages", [])

        # Check if any messages were received
        if not messages:
            logger.info("No new files found in the queue. Exiting.")
            return {"statusCode": 200, "body": json.dumps("No new files to process.")}

        # Log the number of messages found
        logger.info(f"Found {len(messages)} new files to process.")

        # Initialize lists to store file information and receipt handles
        file_keys = []
        receipt_handles = []

        # Process each message to extract S3 file information
        for message in messages:
            body = json.loads(message["Body"])
            for record in body.get("Records", []):
                s3_info = record.get("s3", {})
                bucket_name = s3_info.get("bucket", {}).get("name")
                object_key = s3_info.get("object", {}).get("key")
                # Collect valid bucket and key pairs
                if bucket_name and object_key:
                    file_keys.append({"bucket": bucket_name, "key": object_key})
            # Store receipt handle for message deletion
            receipt_handles.append(message["ReceiptHandle"])

        # Generate a unique execution ID for the Step Function
        execution_id = str(uuid.uuid4())
        sfn_input = {"files": file_keys, "execution_id": execution_id}

        # Log the Step Function execution start with input details
        logger.info(
            f"Starting Step Function execution {execution_id} with input: {json.dumps(sfn_input)}"
        )
        # Trigger the Step Function execution with the collected files
        sfn.start_execution(
            stateMachineArn=step_function_arn, input=json.dumps(sfn_input)
        )

        # Delete processed messages from the SQS queue
        for handle in receipt_handles:
            sqs.delete_message(QueueUrl=sqs_queue_url, ReceiptHandle=handle)

        # Log successful pipeline start and message deletion
        logger.info(
            f"Successfully started pipeline and deleted {len(messages)} messages from the queue."
        )
        # Return success response with the number of files processed
        return {
            "statusCode": 200,
            "body": json.dumps(
                f"Successfully started pipeline for {len(file_keys)} files."
            ),
        }

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise e
