import logging
import time
import os
from functools import wraps
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Centralized Logging Setup
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("energy_ingestion")

def retry_on_failure(retries=3, delay=2, backoff=2):
    """
    Decorator for adding exponential backoff retries to API calls.
    :param retries: Number of attempts
    :param delay: Initial delay in seconds
    :param backoff: Multiplier for delay after each failure
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == retries - 1:
                        logger.error(f" Final attempt failed for {func.__name__}: {e}")
                        raise e
                    
                    logger.warning(
                        f" Attempt {i+1} failed for {func.__name__}: {e}. "
                        f"Retrying in {current_delay}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator