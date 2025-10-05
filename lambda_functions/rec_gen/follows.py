import traceback
from bluesky_requests import get_follows
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
users_table = dynamodb.Table('users')

MAX_FOLLOWS = 5000

def get_all_follows(viewer, retry_n):
    """Get all accounts the viewer follows from BlueSky, handling pagination"""
    logger.info(f'Querying Bluesky for follows')
      
    all_follows = []
    cursor = None 
    while len(all_follows) < MAX_FOLLOWS:
        follows, response_cursor = get_follows(viewer, retry_n, cursor=cursor, limit=100)

        if follows is None:
            break
            
        all_follows.extend(follows)
        cursor = response_cursor
        if not cursor:
            break

    follow_dids = [follow['did'] for follow in all_follows]
    logger.info(f"Fetched {len(all_follows)} follows")

    return follow_dids