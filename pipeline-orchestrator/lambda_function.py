import boto3
import json
import os
import logging
import uuid

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
    This function polls an SQS queue and starts a Step Function execution
    with a batch of files and a unique execution_id.
    """
    logger.info("Orchestrator started. Polling SQS queue...")

    try:
        response = sqs.receive_message(
            QueueUrl=sqs_queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=10
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
            for record in body.get("Records", []):
                s3_info = record.get("s3", {})
                bucket_name = s3_info.get("bucket", {}).get("name")
                object_key = s3_info.get("object", {}).get("key")
                if bucket_name and object_key:
                    file_keys.append({"bucket": bucket_name, "key": object_key})
            receipt_handles.append(message["ReceiptHandle"])

        execution_id = str(uuid.uuid4())
        sfn_input = {"files": file_keys, "execution_id": execution_id}

        logger.info(
            f"Starting Step Function execution {execution_id} with input: {json.dumps(sfn_input)}"
        )
        sfn.start_execution(
            stateMachineArn=step_function_arn, input=json.dumps(sfn_input)
        )

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
        raise e
