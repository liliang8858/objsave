"""
Example configuration file for ObSave
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Copy this file to config/local.py and modify as needed.
"""

# Server settings
HOST = "0.0.0.0"
PORT = 8000
DEBUG = False
WORKERS = 4

# Storage settings
STORAGE_PATH = "./data"
MAX_OBJECT_SIZE = 1024 * 1024 * 100  # 100MB
CACHE_SIZE = 1024 * 1024 * 500  # 500MB

# Database settings
DATABASE_URL = "sqlite:///obsave.db"
POOL_SIZE = 20
MAX_OVERFLOW = 10

# Security settings
SECRET_KEY = "your-secret-key-here"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
ALGORITHM = "HS256"

# Monitoring settings
ENABLE_METRICS = True
METRICS_PATH = "/metrics"
LOG_LEVEL = "INFO"
LOG_FILE = "logs/obsave.log"

# Rate limiting
RATE_LIMIT_ENABLED = True
RATE_LIMIT = "100/minute"
