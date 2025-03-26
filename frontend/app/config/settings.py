import os
#from dotenv import load_dotenv

# Load environment variables from .env file if it exists
#load_dotenv()

# App settings
ASSETS_PATH = "assets"
DEBUG = os.environ.get("DEBUG", "False").lower() == "true"

# TiTiler settings
TITILER_ENDPOINT = os.environ["TITILER_ENDPOINT"]

# Database settings
PG_USER = os.environ["PG_USER"]
PG_HOST = os.environ["PG_HOST"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_PORT = os.environ["PG_PORT"]

# Download settings
MAX_DOWNLOADS = int(os.environ.get("MAX_DOWNLOADS", "5"))
MAX_DOWNLOAD_AREA = float(os.environ.get("MAX_DOWNLOAD_AREA", "1000.0"))

# Analysis settings (AI Chat Window)
MAX_CHATS = int(os.environ.get("MAX_CHATS", "5"))

# AWS S3 settings
S3_BUCKET = os.environ["S3_BUCKET"]

# AWS Bedrock Settings
AGENT_ID = os.environ["AGENT_ID"]
AGENT_ALIAS_ID = os.environ["AGENT_ALIAS_ID"]