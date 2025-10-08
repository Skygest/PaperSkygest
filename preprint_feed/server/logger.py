import logging
from logging.handlers import RotatingFileHandler
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler for stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Create file handler for CloudWatch to pick up
file_handler = RotatingFileHandler('/var/log/preprint-bluesky-feed/errors.log', maxBytes=10485760, backupCount=5)
file_handler.setLevel(logging.ERROR)

# Create formatters and add them to handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)