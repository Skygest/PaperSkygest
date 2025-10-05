from atproto import verify_jwt, IdResolver
from atproto.exceptions import TokenInvalidSignatureError, TokenDecodeError
import time
import logging

_ID_RESOLVER = IdResolver()

_AUTHORIZATION_HEADER_NAME = 'authorization'
_AUTHORIZATION_HEADER_VALUE_PREFIX = 'Bearer '

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class AuthorizationError(Exception):
    ...

def validate_auth(event):
    """Validate authorization header.

    Args:
        request: The request to validate.

    Returns:
        :obj:`str`: Requester DID.

    Raises:
        :obj:`AuthorizationError`: If the authorization header is invalid.
    """
    auth_header = event['headers'].get(_AUTHORIZATION_HEADER_NAME, None)

    if not auth_header:
        return 0 #raise AuthorizationError('Authorization header is missing')

    if not auth_header.startswith(_AUTHORIZATION_HEADER_VALUE_PREFIX):
        raise AuthorizationError('Invalid authorization header')

    jwt = auth_header[len(_AUTHORIZATION_HEADER_VALUE_PREFIX) :].strip()

    attempts = 0
    while attempts < 5:
        try:
            verify_response = verify_jwt(jwt, get_signing_key_callback=_ID_RESOLVER.did.resolve_atproto_key)
            return verify_response.iss
        except (TokenInvalidSignatureError, TokenDecodeError) as e:
            raise AuthorizationError('Invalid signature') from e
        except Exception as e:
            attempts += 1
            logger.error(f'Error verifying JWT: {e}. Retrying {attempts}/5...')
            if attempts >= 5:
                raise AuthorizationError('Failed to verify JWT') from e
            time.sleep(0.1*(2**attempts))
