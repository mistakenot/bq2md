"""Configuration handling for bq2md."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Check if Google Cloud credentials are set
def check_credentials():
    """Check if Google Cloud credentials are properly configured."""
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if not creds_path:
        # Check for application default credentials
        adc_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        if os.path.exists(adc_path):
            return True, "Using application default credentials."
        return False, "GOOGLE_APPLICATION_CREDENTIALS environment variable not set and no application default credentials found."
    
    if not Path(creds_path).exists():
        return False, f"Credentials file not found at: {creds_path}"
    
    return True, "Credentials configured correctly." 