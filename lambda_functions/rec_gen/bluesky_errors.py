import boto3
import time
import json
import os

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
users_table = dynamodb.Table('users')

sqs = boto3.resource('sqs')
recgen_queue = sqs.Queue(os.environ.get('RECGEN_QUEUE'))

RETRY_WAIT = 60

def format_recgen_message(viewer, retry_n):
    return {
        "users": [viewer],
        "generate_agg": False,
        "retry_n": retry_n
    }

def dispatch_recgen_message(viewer, logger, retry_n):
    message = format_recgen_message(viewer, retry_n)
    recgen_queue.send_message(MessageBody=json.dumps(message))
    logger.info('Sent individual user rec-gen task to SQS queue')

def check_deactivated(content):
    error = content.error
    message = content.message
    if (error == 'AccountDeactivated' and 'Account is deactivated' in message) or (error == 'InvalidRequest' and ('Actor not found' in message or 'Profile not found' in message)):
        return True

def check_deactivated_http(error, message):
    return (error == 'AccountDeactivated' and 'Account is deactivated' in message) or (error == 'InvalidRequest' and ('Actor not found' in message or 'Profile not found' in message))

def check_suspended_http(error, message):
    return error == 'AccountTakedown'

def handle_deactivated(user_did, logger):
    logger.error(f'Deactivating user {user_did}')
    users_table.update_item(
        Key={
            'user_did': user_did
        },
        UpdateExpression="SET deactivated=:a",
        ExpressionAttributeValues={
            ':a': True
        }
    )
    logger.info('Viewer is deactivated')

def handle_upstream_failure(user_did, logger, retry_n):
    if retry_n >= 4:
        logger.error("Unhandled recommendation generation error: please investigate.")
        logger.error(f"Unhandled recommendation generation error: Error fetching follows batch for viewer {user_did}")
        logger.error(f"Unhandled recommendation generation error: Retried due to timeout or upstream failure >= 4 times.")

    time.sleep(RETRY_WAIT*(2**retry_n))
    retry_n = retry_n + 1
    dispatch_recgen_message(user_did, logger, retry_n)

def check_timeout(response):
    return response.status_code == 504 or response.status_code == 408

def handle_error_http(user_did, response, logger, retry_n):
    if check_timeout(response):
        logger.error(f'Upstream failure; retrying after {RETRY_WAIT} seconds')
        handle_upstream_failure(user_did, logger, retry_n)
        return
    response_json = response.json()
    error = response_json.get('error', None)
    message = response_json.get('message', None)
    if check_deactivated_http(error, message):
        handle_deactivated(user_did, logger)
    elif check_suspended_http(error, message):
        logger.error(f'User {user_did} is suspended. Skipping for now.')
    elif error and error == 'UpstreamFailure':
        logger.error(f'Upstream failure; retrying after {RETRY_WAIT} seconds')
        handle_upstream_failure(user_did, logger, retry_n)
    else:
        logger.error("Unhandled recommendation generation error: please investigate.")
        logger.error(f"Unhandled recommendation generation error: Error fetching follows batch for viewer {user_did}: {error}")
        logger.error(f"Unhandled recommendation generation error: Message: {message}")

