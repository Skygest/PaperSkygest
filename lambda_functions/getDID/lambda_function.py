import json
import os
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

URIS = [os.environ.get('PAPERSKYGEST_URI'), os.environ.get('ALGFEED_URI')]

def handle_feed_generator_request(event):
    feeds = [{'uri': uri} for uri in URIS]
    return {
        'statusCode': 200,
        'encoding': 'application/json',
        'body': json.dumps({
            'did': f'did:web:{HOSTNAME}',
            'feeds': feeds
        })
    }


def handle_did_request(event):
    logger.info(f"Received event: {json.dumps(event)}") 
    return {
        'statusCode': 200,
        'encoding': 'application/json',
        'body': json.dumps({
            '@context': ['https://www.w3.org/ns/did/v1'],
            'id': f'did:web:{os.environ.get('HOSTNAME')}',
            'service': [
                {
                    'id': '#bsky_fg',
                    'type': 'BskyFeedGenerator',
                    'serviceEndpoint': f'https://{os.environ.get('HOSTNAME')}'
                }]
                })
    }

def lambda_handler(event, context):
    path = event['path']

    if path == '/.well-known/did.json':
        return handle_did_request(event)
    elif path == '/xrpc/app.bsky.feed.describeFeedGenerator':
        return handle_feed_generator_request(event)
    
    return {
        'statusCode': 404,
        'body': json.dumps({'error': 'Not found'})
    }
