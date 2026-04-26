# auth.py
import os
import pyotp
from dotenv import load_dotenv
from SmartApi import SmartConnect
from logzero import logger

# Load .env file
load_dotenv()
ANGEL_API_KEY=os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID=os.getenv("ANGEL_CLIENT_ID")
ANGEL_CLIENT_PASSWORD=os.getenv("ANGEL_CLIENT_PASSWORD")
ANGEL_TOTP_SECRET=os.getenv("ANGEL_TOTP_SECRET")
api_key = ANGEL_API_KEY
client_id = ANGEL_CLIENT_ID
client_pwd = ANGEL_CLIENT_PASSWORD
totp_secret = ANGEL_TOTP_SECRET

if not all([api_key, client_id, client_pwd, totp_secret]):
    raise ValueError("Missing credentials in .env file")

# Initialize Smart API
smartApi = SmartConnect(api_key)

# Generate TOTP
try:
    totp = pyotp.TOTP(totp_secret).now()
except Exception as e:
    logger.error("Invalid TOTP Secret")
    raise e

# Generate Session
session = smartApi.generateSession(client_id, client_pwd, totp)
if not session["status"]:
    logger.error(f"Login failed: {session}")
    raise Exception("Login failed")

logger.info("Login Successful")

authToken = session["data"]["jwtToken"]
refreshToken = session["data"]["refreshToken"]

# Fetch feed token
feedToken = smartApi.getfeedToken()

# Fetch profile
profile = smartApi.getProfile(refreshToken)

# Print all details
print("\n===== ANGEL API SESSION DETAILS =====")
print(f"API Key       : {api_key}")
print(f"Client ID     : {client_id}")
print(f"Auth Token    : {authToken}")
print(f"Refresh Token : {refreshToken}")
print(f"Feed Token    : {feedToken}")

print("\n===== USER PROFILE =====")
if "data" in profile:
    for k, v in profile["data"].items():
        print(f"{k:15} : {v}")
else:
    print("Profile fetch failed:", profile)

print("=====================================\n")

# Export variables for use in other scripts
__all__ = ["smartApi", "authToken", "refreshToken", "feedToken", "api_key", "client_id"]
