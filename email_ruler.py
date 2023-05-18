import os
import json
import psycopg2
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

# Set up Gmail API credentials
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'

# Set up PostgreSQL database connection
DB_NAME = 'email_db'
DB_USER = 'postgresql'
DB_PASSWORD = 'password'
DB_PORT = 5432
DB_HOST = 'localhost'



# Load rules from JSON file
RULES_FILE = 'rules.json'


def authenticate():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = creds_from_file()
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        save_creds_to_file(creds)
    return creds


def creds_from_file():
    creds = None
    with open(TOKEN_FILE, 'r') as token:
        creds = json.load(token)
    return creds


def save_creds_to_file(creds):
    with open(TOKEN_FILE, 'w') as token:
        json.dump(creds, token)


def fetch_emails(service):
    results = service.users().messages().list(userId='me', labelIds=['INBOX']).execute()
    messages = results.get('messages', [])
    if not messages:
        print('No new messages found.')
        return []
    else:
        email_list = []
        for message in messages:
            msg = service.users().messages().get(userId='me', id=message['id']).execute()
            email_list.append(msg)
        return email_list


def store_emails(emails):
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cursor = conn.cursor()
    for email in emails:
        # Extract relevant email fields
        email_id = email['id']
        from_address = next(h['value'] for h in email['payload']['headers'] if h['name'] == 'From')
        subject = email['payload']['headers'][16]['value']
        message = email['snippet']
        received_date = email['internalDate']

        # Insert email into database
        cursor.execute(
            "INSERT INTO emails (email_id, from_address, subject, message, received_date) VALUES (%s, %s, %s, %s, %s)",
            (email_id, from_address, subject, message, received_date))

    conn.commit()
    cursor.close()
    conn.close()


def process_emails():
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cursor = conn.cursor()

    # Load rules from JSON file
    with open(RULES_FILE, 'r') as rules_file:
        rules = json.load(rules_file)

    for rule in rules:
        field = rule['field']
        predicate = rule['predicate']
        value = rule['value']
        action = rule['action']

        # Build SQL query based on rule conditions
        if predicate == 'contains':
            query = f"SELECT * FROM emails WHERE {field} LIKE '%{value}%'"
        elif predicate == 'not equals':
            query = f"SELECT * FROM emails WHERE {field} != '{value}'"
        elif predicate == 'less than':
            query = f"SELECT * FROM emails WHERE {field} < '{value}'"
        else:
            # Handle other predicates if needed
            query = ""

        # Execute the query and process the matching emails
        cursor.execute(query)
        matching_emails = cursor.fetchall()
        for email in matching_emails:
          # Perform actions on the matching emails based on the specified action
          if action == 'mark_as_read':
              # Code to mark the email as read
              email_id = email[0]  # Assuming the email_id is the first column in the SELECT query
              # Implement the logic to mark the email as read using the Gmail API
      
          elif action == 'mark_as_unread':
              # Code to mark the email as unread
              email_id = email[0]  # Assuming the email_id is the first column in the SELECT query
              # Implement the logic to mark the email as unread using the Gmail API
      
          elif action == 'move_message':
              # Code to move the email to a different label/folder
              email_id = email[0]  # Assuming the email_id is the first column in the SELECT query
              destination_label = 'MyFolder'  # Replace with the desired label/folder name
              # Implement the logic to move the email to the specified label/folder using the Gmail API


    conn.commit()
    cursor.close()
    conn.close()



# Main execution flow
if __name__ == '__main__':
    # Authenticate with Gmail API
    creds = authenticate()

    # Create a Gmail service instance
    service = build('gmail', 'v1', credentials=creds)

    # Fetch emails from Gmail
    emails = fetch_emails(service)

    # Store emails in the database
    store_emails(emails)

    # Process emails based on rules and actions
    process_emails()

