import logging
import boto3
import os
from recommendation_generation import generate_recommendations_batch

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.resource('sqs')
recgen_queue = sqs.Queue(os.environ.get('RECGEN_QUEUE'))

def lambda_handler(event, context):
    logger.info("Initiating message sending")
    generate_recommendations_batch(recgen_queue)
    logger.info("Finished sending messages")