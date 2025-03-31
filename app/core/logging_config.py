from logging.handlers import RotatingFileHandler
import logging
import sys

# Define log format
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler("logs/app.log", maxBytes=5_000_000, backupCount=5),
    ],
)

# Create logger instance
logger = logging.getLogger("fastapi_app")
