import json
import os
import logging
import boto3
import traceback
from datetime import datetime, timezone
from bluesky_requests import get_profile

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
users_table = dynamodb.Table('users')
access_agg_table = dynamodb.Table('user_accesses_agg')
access_table = dynamodb.Table('user_accesses')

CONSENT_ACTIVE = True

def update_access_agg_db(cursor, access_date, user_did):
    if cursor == 0:
        past_user_info = access_agg_table.get_item(Key={'user_did': user_did})
        if past_user_info and past_user_info.get('Item'):
            logger.info('User already in database')
            item = past_user_info.get('Item')
            n_accesses = item.get('access_count', 0)
            n_consent = item.get('consent_accesses', 0)
        else:
            n_accesses = 0
            n_consent = -1

        consent_accesses = n_consent + 1 if CONSENT_ACTIVE else 0

        access_agg_table.put_item(Item={
            'user_did': user_did,
            'last_access': access_date,
            'access_count': n_accesses + 1,
            'consent_accesses': consent_accesses
        })
        logger.info('Finished aggregate access database')

def update_user_db(message, user_did):
    # api call to get user handle
    profile_response = get_profile(user_did)

    dynamo_response = users_table.get_item(Key={'user_did': user_did})
    user_item = dynamo_response.get('Item', None)

    # if the user is not already in the database, add to the database (if active)
    if not user_item:
        if profile_response:
            users_table.put_item(Item={
                'user_did': user_did, 
                'user_display_name': profile_response.get('displayName', None),
                'user_handle': profile_response['handle'],
                'research_remove': False,
                'deactivated': False
            })
            logger.info(f'Added user {user_did} to user database')
            return
        else:
            # if the user is not in the database + the user is deactivated, do nothing
            return

    # otherwise, update user in the database
    if profile_response:
        # user is active
        users_table.update_item(Key={'user_did': user_did},
            UpdateExpression='SET user_display_name = :val1, user_handle = :val2, deactivated = :val3',
            ExpressionAttributeValues={
                ':val1': profile_response.get('displayName', None),
                ':val2': profile_response['handle'],
                ':val3': False
            })
    else:
        # user is deactivated
        users_table.update_item(
            Key={'user_did': user_did}, 
            UpdateExpression='SET deactivated = :val',
            ExpressionAttributeValues={':val': True}
        )

    logger.info('Finished updating user database')


def update_access_db(user_did, recs, access_date, default_from, cursors):
    # check that the user has not contacted us asking for removal
    response = users_table.get_item(Key={'user_did': user_did})
    user_item = response.get('Item', None)
    research_remove = user_item.get('research_remove', False)

    access_date_save = access_date if access_date else datetime.now(timezone.utc).isoformat()
    access_date_day = datetime.fromisoformat(access_date_save).date().isoformat()

    if not research_remove:
        # log the access in the access DynamoDB table
        access_table.put_item(Item={
            'user_did': user_did,
            'access_date': access_date if access_date else datetime.now(timezone.utc).isoformat(),
            'access_date_day': access_date_day, 
            'recs_shown': recs,
            'start_cursor': cursors[0] if cursors else None,
            'end_cursor': cursors[1] if cursors else None,
            'default_from': int(default_from)
        })
        logger.info('Finished updating access database')
    else:
        logger.info('User has requested removal from research; access not logged')

def parse_event(event):
    records = event['Records']
    entry = records[0]
    body = json.loads(entry['body'])
    user_did = body['viewer']
    recs = body['recs']
    active = body['active']
    default_from = body.get('default_from', None)
    cursors = body.get('cursors', None)
    access_date = body.get('access_date', None)
    return user_did, recs, active, access_date, default_from, cursors, entry['body']

def lambda_handler(event, context):
    user_did, recs, active, access_date, default_from, cursors, message = parse_event(event)

    try:
        if active:
            update_user_db(message, user_did)
            update_access_agg_db(cursors[0] if cursors else 0, access_date, user_did)
            update_access_db(user_did, recs, access_date, default_from, cursors)

        return {
            'statusCode': 200,
            'body': json.dumps('Finished correctly.')
        }
    except Exception as e:
        logger.error(f"Unhandled postprocessing error: Unexpected error: {str(e)}")
        logger.error(f"Unhandled postprocessing error: Error type: {type(e)}")
        logger.error(f"Unhandled postprocessing error: Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }