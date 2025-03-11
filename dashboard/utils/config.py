import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_URI = os.getenv("DB_URI")
