import os
from dotenv import load_dotenv
from aws_api_handler import update_meter_data, download_file_from_s3

# Load environment variables from .env file
load_dotenv()

# Configuration
API_KEY = os.getenv("API_KEY")
SITE_ID = os.getenv("SITE_ID")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
FILE_KEY = os.getenv("FILE_KEY")

# Update parquet file with most recent API data
update_meter_data()

