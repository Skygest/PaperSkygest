import json
import logging
import boto3
import traceback
import os
from datetime import datetime, timezone
from auth import AuthorizationError, validate_auth
from recommendation_generation import get_recommendations, get_agg_feed
from alg_recommendations import get_alg_recs

CURSOR_EOF = 'eof'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Setting up')

sqs = boto3.resource('sqs')
postprocess_queue = sqs.Queue(os.environ.get('POSTPROCESS_QUEUE_ARN'))
recgen_queue = sqs.Queue(os.environ.get('RECGEN_QUEUE_ARN'))

FIXED_POSTS = {
    'no_feed': 'at://did:plc:uaadt6f5bbda6cycbmatcm3z/app.bsky.feed.post/3ljjnqm4vl72l',
    'auth': 'at://did:plc:uaadt6f5bbda6cycbmatcm3z/app.bsky.feed.post/3ljjm35xgu72n',
    'new_user': 'at://did:plc:uaadt6f5bbda6cycbmatcm3z/app.bsky.feed.post/3ljsdllvnu42d'
}

ALG_FEED_DID = os.environ.get('ALG_FEED_DID')

logger.info('Finished setting up')

def get_limit(event):
    limit = event['queryStringParameters']['limit']
    logger.info(f'Limit: {limit}')
    return int(limit)

def get_cursor(event):
    cursor = event['queryStringParameters'].get('cursor')
    if not cursor:
        cursor = 0

    if cursor == CURSOR_EOF:
        return CURSOR_EOF

    return int(cursor)

def format_post(post):
    # check if post is a dict
    if isinstance(post, dict):
        return post

    return {'post': post}

def format_response(cursor, all_posts):
    response = {
        'cursor': str(cursor),
        'feed': [format_post(post) for post in all_posts]
    }
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(response)
    }

def format_postprocessing_message(viewer, recs, active, default_from, cursors):
    return {
        "viewer": viewer,
        "recs": recs,
        "active": active,
        "access_date": datetime.now(timezone.utc).isoformat(),
        "default_from": int(default_from),
        "cursors": cursors
    }

def format_recgen_message(viewer):
    return {
        "users": [viewer],
        "generate_agg": False
    }

def dispatch_recgen_message(viewer):
    message = format_recgen_message(viewer)
    recgen_queue.send_message(MessageBody=json.dumps(message))
    logger.info('Sent individual user rec-gen task to SQS queue')

def dispatch_postprocessing_message(viewer, rec_posts, limit, default_from, cursors):
    message = format_postprocessing_message(viewer, rec_posts, limit > 1, default_from, cursors)
    postprocess_queue.send_message(MessageBody=json.dumps(message))
    logger.info('Sent postprocessing task to SQS queue')

def parse_event(event):
    limit = get_limit(event)
    cursor = get_cursor(event)

    return limit, cursor

def serve_recommendations(event, context):
    # get the limit and cursor from the API request
    limit = get_limit(event) # how many posts we're supposed to return
    cursor = get_cursor(event)

    # if the end of the feed is reached, return immediately
    if cursor == CURSOR_EOF:
        return format_response(CURSOR_EOF, [])

    # authenticate user
    try:
        viewer = validate_auth(event)
        
        # if not authenticated, return the default feed
        if viewer == 0:
            rec_posts = [FIXED_POSTS['auth']]
            agg_feed, return_cursor = get_agg_feed(cursor, limit)
            if agg_feed:
                rec_posts.extend(agg_feed)

            return format_response(return_cursor, rec_posts)
        logger.info('Authenticated successfully')
    except AuthorizationError:
        logger.error("Authorization error")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 401,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'message': 'Unauthorized'
        }

    if 'queryStringParameters' not in event:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': 'Missing query string parameters'})
        }
    else:
        if event['queryStringParameters']['feed'] == ALG_FEED_DID:
            return format_response(CURSOR_EOF, get_alg_recs(viewer))
        
    try:
        logger.info("Getting posts from recommendations database")
        rec_posts, return_cursor, default_from, new_user = get_recommendations(viewer, cursor, limit, logger)
        logger.info(f"Got {len(rec_posts)} posts from recommendations database")
    
        # send the "wait a moment" post while recommendations are being generated
        if new_user:
            rec_posts = [FIXED_POSTS['new_user']]

        # trigger recommendation regeneration + access logging
        dispatch_recgen_message(viewer)
        logger.info('Dispatching postprocessing message')
        dispatch_postprocessing_message(viewer, rec_posts, limit, default_from, [cursor, return_cursor])

        logger.info("Serving recommendations")
        
        if not return_cursor:
            return_cursor = CURSOR_EOF

        return format_response(return_cursor, rec_posts)
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }

def is_warmup(event):    
    return 'keep_warm' in event and event['keep_warm']

def lambda_handler(event, context):
    logger.info(f"Event: {event}")
    logger.info(f"Context: {context}")
    if is_warmup(event):
        # we trigger a warmup event every 4 minutes to keep the Lambda warm
        logger.info("Warmed up")
        return
    else:
        logger.info("Serving recommendations")
        return serve_recommendations(event, context)