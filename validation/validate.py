import pandas as pd
import boto3
import os
from datetime import datetime
import json

# Initialize AWS clients
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

BUCKET_NAME = "lab6-realtime-ecommerce-pipelines"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:985539772768:lab6-pipeline-failure-notifications"
