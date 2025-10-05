import boto3
import json
import logging
import random

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
users_table = dynamodb.Table('users')

def user_is_active(item):
    if 'deactivated' not in item:
        return True
    deactivated = item.get('deactivated') 
    if deactivated:
        logger.info(f'User {item["user_did"]} is deactivated')
    return not deactivated

def get_all_users():
    logger.info('Getting all users')
    items = []
    response = users_table.scan(
        ProjectionExpression='user_did,deactivated',
    )
    items.extend(response['Items'])

    # Handle pagination only if we don't have enough items
    while 'LastEvaluatedKey' in response:
        response = users_table.scan(
                ProjectionExpression='user_did,deactivated',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
        items.extend(response['Items'])

    users = [item['user_did'] for item in items if user_is_active(item)]
    inactive_users = [item['user_did'] for item in items if not user_is_active(item)]
    logger.info(f'Found {len(users)} users: {users}')
    logger.info(f'Found {len(inactive_users)} inactive users: {inactive_users}')

    return users

def format_recgen_message(users, batch_idx, generate_agg):
    return {
        "users": users,
        "batch_idx": batch_idx,
        "generate_agg": generate_agg
    }

def format_recgen_batch_entry(users, batch_idx, generate_agg):
    msg = format_recgen_message(users, batch_idx, generate_agg)
    entry = {
        'Id': f'msg_{batch_idx}', 
        'MessageBody': json.dumps(msg)
    }
    return entry

def dispatch_recgen_messages_batch(queue, batch_sets):
    entries = [format_recgen_batch_entry(users, batch_idx, generate_agg) for users, batch_idx, generate_agg in batch_sets]
    queue.send_messages(Entries=entries)

def generate_recommendations_batch(queue, batch_size=20):
    logger.info('Generating recommendations')
    users = get_all_users()
    
    logger.info('Shuffling users')
    random.shuffle(users)

    batch = []
    batch_idx = 0
    batch_sets = []
    for i, user in enumerate(users):
        batch.append(user)
        if len(batch) >= batch_size:
            batch_idx += 1
            batch_sets.append((batch, batch_idx, batch_idx == 1))
            batch = []
        
        if len(batch_sets) >= 10:
            logger.info('Dispatching batch set')
            dispatch_recgen_messages_batch(queue, batch_sets)
            batch_sets = []

    if len(batch) > 0 or len(batch_sets) > 0:
        if len(batch) > 0:
            batch_idx += 1
            batch_sets.append((batch, batch_idx, batch_idx == 1))
        logger.info(f'Dispatching final batch set')
        dispatch_recgen_messages_batch(queue, batch_sets)