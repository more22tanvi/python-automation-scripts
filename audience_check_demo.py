import os
import psycopg2
import requests
from datetime import datetime

# Load environment variables from a .env file or directly from the environment
from dotenv import load_dotenv
load_dotenv()

# Database connection
try:
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit(1)

# Slack Bot User OAuth Access Token
slack_token = os.getenv('SLACK_BOT_TOKEN')
channel_id = os.getenv('SLACK_CHANNEL_ID')

# Function to send message to Slack
def send_to_slack(message):
    url = os.getenv('SLACK_WEBHOOK_URL')
    headers = {
        'Authorization': f'Bearer {slack_token}',
        'Content-Type': 'application/json'
    }
    payload = {
        'channel': channel_id,
        'text': message,
        'mrkdwn': True
    }
    response = requests.post(url, headers=headers, json=payload)
    print(response.text)
    if not response.ok or not response.json().get('ok'):
        raise ValueError(f'Request to Slack returned an error: {response.text}')

try:
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE TEMP TABLE temp_results (result TEXT);")

            cursor.execute("""
            DO $$
            DECLARE
                account_record RECORD;
                total_audience_count INTEGER;
                updated_today_count INTEGER;
                not_updated_today_count INTEGER;
                latest_not_updated TIMESTAMP;
                result TEXT := '';
            BEGIN
                FOR account_record IN 
                    SELECT id AS account_id, display_name
                    FROM pf.account
                    WHERE id IN (
                        221218718049371529,
                        318376149853931305,
                        189857344031557017,
                        308857572440410099,
                        196436990147692145,
                        353789014706227026,
                        328385381068178508
                    )
                LOOP
                    PERFORM set_account(account_record.account_id);

                    SELECT COUNT(*) INTO total_audience_count
                    FROM pf.audiences
                    WHERE account_id = account_record.account_id
                    AND deleted_at IS NULL
                    AND is_used = 'false'
                    AND slug NOT LIKE 'rt_coll%'
                    AND slug NOT LIKE 'rt_prod%';

                    SELECT COUNT(*) INTO updated_today_count
                    FROM pf.audiences
                    WHERE account_id = account_record.account_id
                    AND deleted_at IS NULL
                    AND is_used = 'false'
                    AND updated_at >= CURRENT_DATE
                    AND slug NOT LIKE 'rt_coll%'
                    AND slug NOT LIKE 'rt_prod%';

                    SELECT COUNT(*), MAX(updated_at) INTO not_updated_today_count, latest_not_updated
                    FROM pf.audiences
                    WHERE account_id = account_record.account_id
                    AND deleted_at IS NULL
                    AND is_used = 'false'
                    AND updated_at < CURRENT_DATE
                    AND slug NOT LIKE 'rt_coll%'
                    AND slug NOT LIKE 'rt_prod%';

                    result := result || format(
                        '%s | %s | %s | %s | %s (%s)\n',
                        account_record.display_name,
                        account_record.account_id,
                        total_audience_count::TEXT,
                        updated_today_count::TEXT,
                        not_updated_today_count::TEXT,
                        COALESCE(latest_not_updated::TEXT, 'N/A')
                    );
                END LOOP;

                INSERT INTO temp_results (result) VALUES (result);
            END $$;
            """)

            cursor.execute("SELECT result FROM temp_results;")
            result = cursor.fetchone()[0]

            # Create the complete message with header and table
            slack_message = "📊 Daily Audience Update Summary\n\n"  # Add the header
            slack_message += "```\n"
            slack_message += "| Account Name        | Account ID          | Total Audiences | Updated Today | Not Updated Today (Latest Updated At) |\n"
            slack_message += "|---------------------|---------------------|-----------------|---------------|---------------------------------------|\n"

            for line in result.split('\n'):
                if line.strip():
                    display_name, account_id, total_count, updated_today, not_updated_today_latest = line.split('|')
                    slack_message += f"| {display_name.strip():<20} | {account_id.strip():<19} | {total_count.strip():<16} | {updated_today.strip():<13} | {not_updated_today_latest.strip()} |\n"
            
            slack_message += "```"

    # Print the result to the terminal
    print(slack_message)

    # Send to Slack
    send_to_slack(slack_message)

except Exception as e:
    print(f"Error executing SQL: {e}")

finally:
    if conn:
        conn.close()
