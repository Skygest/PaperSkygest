import requests
from bluesky_errors import handle_error_http
import logging
import traceback

PUBLIC_API_HOSTNAME = "https://public.api.bsky.app"
BASE_URL = PUBLIC_API_HOSTNAME + "/xrpc"

logger = logging.getLogger()
logger.setLevel(logging.INFO)
        
def get_follows(actor, retry_n, cursor, limit=100):
    url = '/app.bsky.graph.getFollows'
    endpoint = BASE_URL + url
    params = {
        "actor": actor,
        "cursor": cursor,
        "limit": limit
    }
    try:
        response = requests.get(endpoint, params=params)
        if response.status_code != 200:
            handle_error_http(actor, response, logger, retry_n)
            return None, None

        response_json = response.json()
        follows = response_json.get('follows', None)
        if follows is None:
            handle_error_http(actor, response, logger, retry_n)
            return None, None

        cursor = response_json.get('cursor', None)

        return follows, cursor
    except Exception as e:
        logger.error(f"Unhandled recommendation generation error: Unhandled exception in API call to getFollows for user {actor}")
        logger.error(f"Unhandled recommendation generation error: Traceback: {traceback.format_exc()}")
        raise e
