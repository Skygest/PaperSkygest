import json
import logging
from recommendation_generation import generate_recommendations_users

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def parse_event(event):
    records = event['Records']
    entry = records[0]
    body = json.loads(entry['body'])
    users = body['users']
    generate_agg = body['generate_agg']
    batch_idx = body.get('batch_idx', None)
    retry_n = body.get('retry_n', 0)
    return users, generate_agg, batch_idx, retry_n

def lambda_handler(event, context):
    users, generate_agg, batch_idx, retry_n = parse_event(event)
    if batch_idx is not None:
        logger.info(f'BATCH LOG: Processing batch {batch_idx}')
        scheduled_generation = True
    else:
        scheduled_generation = False

    if len(users) == 1:
        logger.info(f'Processing user {users[0]}')

    generate_recommendations_users(users, generate_agg, retry_n, scheduled_generation)