from dotenv import load_dotenv
import os
from supabase import create_client

# Load .env from the project root
load_dotenv()

# Fetch credentials from environment
url = os.environ["SUPABASE_URL"]
key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase = create_client(url, key)
print("✅ Supabase connection successful!")
