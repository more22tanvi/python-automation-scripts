import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv('DB_HOST')  # Database host
DB_NAME = os.getenv('DB_NAME')  # Database name
DB_USER = os.getenv('DB_USER')  # Database user
DB_PASS = os.getenv('DB_PASS')  # Database password
PORT = os.getenv('DB_PORT')     # Database port

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv('GOOGLE_CREDS_FILE'), scope)
client = gspread.authorize(creds)

# Access the Google Sheet
sheet = client.open(os.getenv('GOOGLE_SHEET_NAME')).sheet1  # Ensure this is the correct sheet name
sheet_url = sheet.url  # Get the URL of the Google Sheet

# List of account IDs
account_ids = [
    221218718049371529,
    318376149853931305,
    189857344031557017,
    308857572440410099,
    196436990147692145,
    353789014706227026,
    328385381068178508
]

# SQL queries for fetching errors
email_query = """
SELECT DISTINCT ON (errors->>'code') 
    errors->>'code' AS Error_Code, 
    errors->>'message' AS Error_Message, 
    COUNT(*) OVER (PARTITION BY errors->>'code') AS Error_Count
FROM 
    pf.email_queue_id_status eqis 
WHERE 
    account_id = %s 
    AND status = 'failed'
    AND status_update_at >= NOW() - INTERVAL '24 hours'
ORDER BY 
    errors->>'code', 
    RANDOM();  -- Randomly select one message for each error code
"""

whatsapp_query = """
    SELECT DISTINCT ON (errors->>'code') 
    errors->>'code' AS Error_Code, 
    errors->>'message' AS Error_Message, 
    COUNT(*) OVER (PARTITION BY errors->>'code') AS Error_Count
FROM 
    pf.whatsapp_wamid_status wms
WHERE 
    account_id = %s 
    AND status = 'failed'
    AND status_update_at >= NOW() - INTERVAL '24 hours'
    AND errors IS NOT NULL
ORDER BY 
    errors->>'code', 
    RANDOM();  -- Randomly select one message for each error code
"""

sms_query = """
    SELECT DISTINCT ON (errors->>'code') 
    errors->>'code' AS Error_Code,
    errors->>'error' AS Error_Message,
    COUNT(*) OVER (PARTITION BY errors->>'code') AS Error_Count
FROM 
    pf.sms_queue_id_status
WHERE 
    account_id = %s 
    AND status = 'failed'
    AND status_update_at >= NOW() - INTERVAL '24 hours'
GROUP BY 
    errors->>'code', errors->>'error'
ORDER BY 
    errors->>'code', 
    RANDOM();  -- Randomly select one message for each error code
"""

# Slack Bot User OAuth Access Token
slack_token = os.getenv('SLACK_BOT_TOKEN')  # Make sure to set this in your environment
channel_id = os.getenv('SLACK_CHANNEL_ID')  # Your channel ID

def connect_db():
    """Establishes a connection to the PostgreSQL database."""
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=PORT)

def fetch_display_name(cursor, account_id):
    """Fetches the display name for the given account ID."""
    cursor.execute("SELECT display_name FROM pf.account WHERE id = %s;", (account_id,))
    return cursor.fetchone()

def fetch_errors(cursor, account_id):
    """Fetches errors for Email, WhatsApp, and SMS channels."""
    errors = {'Email': [], 'WhatsApp': [], 'SMS': []}

    # Email Errors
    cursor.execute(email_query, (account_id,))
    for row in cursor.fetchall():
        errors['Email'].append(row)

    # WhatsApp Errors
    cursor.execute(whatsapp_query, (account_id,))
    for row in cursor.fetchall():
        errors['WhatsApp'].append(row)

    # SMS Errors
    cursor.execute(sms_query, (account_id,))
    for row in cursor.fetchall():
        errors['SMS'].append(row)

    return errors

def insert_into_sheet(account_id, display_name, errors):
    """Inserts error data into the Google Sheet in batches to reduce API requests."""
    data_to_write = []  # Collect all data to write in a single batch

    # Add account header
    data_to_write.append([f"Account ID: {account_id}", f"Display Name: {display_name}"])
    
    # Add errors for each channel
    for channel, error_list in errors.items():
        data_to_write.append([f"{channel} Errors:"])  # Channel header
        if error_list:
            for error in error_list:
                data_to_write.append([error[0], error[1], error[2]])  # Error Code, Error Message, Count
        else:
            data_to_write.append(["No errors found."])  # No errors message

    # Write all data in one batch
    sheet.append_rows(data_to_write, value_input_option='RAW')

def send_to_slack(message):
    """Sends a message to a Slack channel."""
    url = os.getenv('SLACK_WEBHOOK_URL')  # Slack webhook URL
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        'channel': channel_id,
        'text': message,
        'mrkdwn': True
    }
    response = requests.post(url, headers=headers, json=payload)
    print(response.text)  # Print the full response for debugging
    if not response.ok or not response.json().get('ok'):
        raise ValueError(f'Request to Slack returned an error: {response.text}')

def main():
    """Main function to run the error fetching process."""
    try:
        conn = connect_db()
        cursor = conn.cursor()

        for account_id in account_ids:
            # Set the account context
            cursor.execute("SELECT set_account(%s);", (account_id,))
            display_name = fetch_display_name(cursor, account_id)
            display_name = display_name[0] if display_name else 'N/A'

            errors = fetch_errors(cursor, account_id)

            # Log the errors for debugging
            print(f"\nAccount ID: {account_id}, Display Name: {display_name}")
            print(f"Email Errors: {errors['Email']}")
            print(f"WhatsApp Errors: {errors['WhatsApp']}")
            print(f"SMS Errors: {errors['SMS']}")

            insert_into_sheet(account_id, display_name, errors)  # Insert errors into Google Sheet

        # Send Slack message with the Google Sheet link
        slack_message = f"Daily Error Logging sheet updated. Click the below link to review past 24 hours errors:\n{sheet_url}"
        send_to_slack(slack_message)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
