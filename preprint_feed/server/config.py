import os

SERVICE_DID = os.environ.get('SERVICE_DID', None)
HOSTNAME = os.environ.get('HOSTNAME', None)

if SERVICE_DID is None:
    SERVICE_DID = f'did:web:{HOSTNAME}'


PREPRINT_URI = os.environ.get('PREPRINT_URI')