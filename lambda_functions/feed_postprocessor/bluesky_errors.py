import boto3
import time
import os

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
users_table = dynamodb.Table('users')

sqs = boto3.resource('sqs')
postprocess_queue = sqs.Queue(os.environ['POSTPROCESS_QUEUE'])

RETRY_WAIT = 10

def dispatch_postprocess_message(message, logger):
    postprocess_queue.send_message(MessageBody=message)
    logger.info('Sent individual user postprocessing task to SQS queue')

def check_deactivated(content):
    error = content.error
    message = content.message
    if (error == 'AccountDeactivated' and 'Account is deactivated' in message) or (error == 'InvalidRequest' and ('Actor not found' in message or 'Profile not found' in message)):
        return True

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

def handle_upstream_failure(user_did, message, logger):
    time.sleep(RETRY_WAIT)
    dispatch_postprocess_message(message, logger)

def check_deactivated_http(error, message):
    return (error == 'AccountDeactivated' and 'Account is deactivated' in message) or (error == 'InvalidRequest' and ('Actor not found' in message or 'Profile not found' in message))

def check_suspended_http(error, message):
    return error == 'AccountTakedown'

def handle_error_http(user_did, response, logger):
    response_json = response.json()
    error = response_json.get('error', None)
    message = response_json.get('message', None)
    if check_deactivated_http(error, message):
        handle_deactivated(user_did, logger)
    elif check_suspended_http(error, message):
        logger.error(f'User {user_did} is suspended. Skipping for now.')
    elif error and error == 'UpstreamFailure':
        logger.error(f'Upstream failure; retrying after {RETRY_WAIT} seconds')
        handle_upstream_failure(user_did, logger)
    else:
        logger.error("Unhandled recommendation generation error: please investigate.")
        logger.error(f"Unhandled recommendation generation error: Error fetching follows batch for viewer {user_did}: {error}")
        logger.error(f"Unhandled recommendation generation error: Message: {message}")

