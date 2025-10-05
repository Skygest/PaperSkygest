import requests
from bluesky_errors import handle_error_http
import logging

PUBLIC_API_HOSTNAME = "https://public.api.bsky.app"
BASE_URL = PUBLIC_API_HOSTNAME + "/xrpc"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_profile(actor):
    url = '/app.bsky.actor.getProfile'
    endpoint = BASE_URL + url
    params = {
        "actor": actor
    }
    try:
        response = requests.get(endpoint, params=params)
        response_json = response.json()

        if response.status_code != 200:
            handle_error_http(actor, response, logger)
            return None
        else:
            return response_json
    except Exception as e:
        logger.error(f"Unhandled recommendation generation error: Unhandled exception in API call to getProfile for user {actor}")
        return None