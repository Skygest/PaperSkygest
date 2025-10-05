import boto3
import json
import time
import random

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
recommendations_table = dynamodb.Table('recommendations')
users_table = dynamodb.Table('users')

FOLLOW_RECOMMENDER_NAME = 'recent-following-posts'
FOLLOW_RECOMMENDER_NAME_REPOSTS = 'recent-following-reposts'
FOLLOW_RECOMMENDER_NAME_QUOTEPOSTS = 'recent-following-quoteposts'
FOLLOW_RECOMMENDER_NAME_REPOSTS_QUOTEPOSTS = 'recent-following-repostsandquoteposts'

AGGREGATE_FEED_ID = 'aggregate_feed'

FEED_LIMIT = 5000
NO_DEFAULT = -1

def get_recommender(user_did):
    if (
        user_did == 'did:plc:6ysaocl4wbig54tsqox4a2f5' or
        user_did == 'did:plc:kkhfvbrf4me4ogph35hjx3zc'
    ): 
        return FOLLOW_RECOMMENDER_NAME_REPOSTS_QUOTEPOSTS
    else:
        return FOLLOW_RECOMMENDER_NAME

def get_subset_recs(recs, cursor, limit):
    if len(recs) >= cursor + limit:
        return recs[cursor:cursor+limit], cursor+limit
    else:
        return recs[cursor:], None

def get_agg_feed(cursor, limit):
    response= recommendations_table.get_item(Key={
            'user_did': AGGREGATE_FEED_ID,
            'recommender': AGGREGATE_FEED_ID
        })

    if response and response.get('Item'):
        recs = response['Item']['recommendations']
        return get_subset_recs(recs, cursor, limit)
    else:
        return None, None

def user_is_active(item, logger):
    if 'deactivated' not in item:
        return True
    deactivated = item.get('deactivated') 
    if deactivated:
        logger.info(f'User {item["user_did"]} is deactivated')
    return not deactivated

def get_all_users(logger):
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
                #Limit=batch_size
            )
        items.extend(response['Items'])

    users = [item['user_did'] for item in items if user_is_active(item, logger)]
    inactive_users = [item['user_did'] for item in items if not user_is_active(item, logger)]
    logger.info(f'Found {len(users)} users: {users}')
    logger.info(f'Found {len(inactive_users)} inactive users: {inactive_users}')

    return users

def get_recommendations(user_did, cursor, limit, logger):
    logger.info(f'Getting {limit} recommendations for user {user_did}')

    response = recommendations_table.get_item(Key={
        'user_did': user_did,
        'recommender': get_recommender(user_did)
    })

    # check if this is a new user
    if not response.get('Item'):
        return [], None, NO_DEFAULT, True

    # parse response
    item = response.get('Item')
    recs = item['recommendations']
    default_from = item.get('default_from', NO_DEFAULT)

    # trim recommendations to limit
    subset_recs, return_cursor = get_subset_recs(recs, cursor, limit)

    return subset_recs, return_cursor, default_from, False
        