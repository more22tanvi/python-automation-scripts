import requests
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection
try:
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),          # Database name
        user=os.getenv('DB_USER'),            # Database user
        password=os.getenv('DB_PASSWORD'),    # Database password
        host=os.getenv('DB_HOST'),            # Database host
        port=os.getenv('DB_PORT')             # Database port
    )
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit(1)

# Slack Bot User OAuth Access Token
slack_token = os.getenv('SLACK_BOT_TOKEN')  # Slack bot token
channel_id = os.getenv('SLACK_CHANNEL_ID')  # Slack channel ID

# Function to send message to Slack
def send_to_slack(message):
    url = os.getenv('SLACK_WEBHOOK_URL')  # Slack webhook URL
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
    print(response.text)  # Print the full response for debugging
    if not response.ok or not response.json().get('ok'):
        raise ValueError(f'Request to Slack returned an error: {response.text}')

# Execute SQL query
try:
    with conn:
        with conn.cursor() as cursor:
            # Create a temporary table to hold results
            cursor.execute("CREATE TEMP TABLE temp_results (result TEXT);")

            cursor.execute("""
            DO $$
            DECLARE
                account_record RECORD;
                latest_order_date TIMESTAMP;
                latest_customer_created_at TIMESTAMP;
                hours_since_order INTEGER;
                hours_since_customer INTEGER;
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
                    -- Set the account context
                    PERFORM set_account(account_record.account_id);

                    -- Query for latest order date
                    SELECT MAX(order_date) INTO latest_order_date
                    FROM pf.wh_ecom_order
                    WHERE account_id = account_record.account_id;

                    -- Query for latest customer creation date
                    SELECT MAX(cust_created_at) INTO latest_customer_created_at
                    FROM pf.wh_customer
                    WHERE account_id = account_record.account_id;

                    -- Calculate hours since the latest order and customer creation
                    IF latest_order_date IS NOT NULL THEN
                        hours_since_order := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - latest_order_date)) / 3600;
                    ELSE
                        hours_since_order := NULL;
                    END IF;

                    IF latest_customer_created_at IS NOT NULL THEN
                        hours_since_customer := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - latest_customer_created_at)) / 3600;
                    ELSE
                        hours_since_customer := NULL;
                    END IF;

                    -- Format the results
                    result := result || format(
                        '%s | %s | %s hours ago | %s hours ago\n',
                        account_record.display_name,
                        account_record.account_id,
                        COALESCE(hours_since_order::TEXT, 'N/A'),
                        COALESCE(hours_since_customer::TEXT, 'N/A')
                    );
                END LOOP;

                -- Insert the result into the temporary table
                INSERT INTO temp_results (result) VALUES (result);
            END $$;
            """)

            # Fetch the result from the temporary table
            cursor.execute("SELECT result FROM temp_results;")
            result = cursor.fetchone()[0]

            # Manually construct the table as a string for Slack
            table_str = "```\n"
            table_str += "| Account Name        | Account ID          | Last Order    | Last New Customer |\n"
            table_str += "|---------------------|---------------------|---------------|-------------------|\n"
            
            # Add rows to the table string
            for line in result.split('\n'):
                if line.strip():
                    display_name, account_id, last_order, last_customer = line.split('|')
                    table_str += f"| {display_name.strip():<20} | {account_id.strip():<19} | {last_order.strip():<13} | {last_customer.strip():<18} |\n"
            
            table_str += "```"

    # Custom message before the table
    custom_message = " 📊 Order and Customer Data Latest Sync Report\n\n"

    # Concatenate custom message and table
    full_message = custom_message + table_str

    # Print the message to the terminal
    print(full_message)

    # Send the result to Slack
    send_to_slack(full_message)

except Exception as e:
    print(f"Error executing SQL: {e}")

finally:
    # Close the database connection
    if conn:
        conn.close()
