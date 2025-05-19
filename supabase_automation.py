# supabase_automation.py
import os
import json
import base64
import smtplib
import datetime
import csv
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from supabase import create_client, Client
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Initialize Supabase client
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

# Email configuration
gmail_user = os.environ.get("GMAIL_USER")
gmail_password = os.environ.get("GMAIL_APP_PASSWORD")
report_recipient = os.environ.get("REPORT_RECIPIENT")

def get_total_rows(table_name):
    """Get total number of rows in a table."""
    response = supabase.table(table_name).select("count", count="exact").execute()
    return response.count

def get_calls_made_today():
    """Get total calls made today (assuming you have a calls table with timestamp)."""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    response = supabase.table("calls").select("count", count="exact").eq("date", today).execute()
    return response.count

def get_apollo_credits_saved():
    """Get total Apollo credits saved from Supabase."""
    response = supabase.table("apollo_credits").select("total").execute()
    if response.data:
        return response.data[0]["total"]
    return 0

def get_new_rows_today(table_name):
    """Get count of new rows added today."""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    response = supabase.table(table_name).select("count", count="exact").gte("created_at", today).execute()
    return response.count

def send_daily_report():
    """Generate and send daily report via email."""
    leads_total = get_total_rows("leads_db")
    orgs_total = get_total_rows("orgs_db")
    calls_today = get_calls_made_today()
    apollo_credits = get_apollo_credits_saved()
    new_leads_today = get_new_rows_today("leads_db")
    new_orgs_today = get_new_rows_today("orgs_db")
    
    # Create email
    msg = MIMEMultipart()
    msg["From"] = gmail_user
    msg["To"] = report_recipient
    msg["Subject"] = f"Supabase Daily Report - {datetime.datetime.now().strftime('%Y-%m-%d')}"
    
    body = f"""
    <html>
      <body>
        <h2>Supabase Daily Report</h2>
        <h3>Database Stats:</h3>
        <ul>
          <li>Total Leads: {leads_total}</li>
          <li>Total Organizations: {orgs_total}</li>
          <li>Total Calls Made Today: {calls_today}</li>
          <li>Total Apollo Credits Saved: {apollo_credits}</li>
          <li>New Leads Added Today: {new_leads_today}</li>
          <li>New Organizations Added Today: {new_orgs_today}</li>
        </ul>
      </body>
    </html>
    """
    
    msg.attach(MIMEText(body, "html"))
    
    # Send email
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(gmail_user, gmail_password)
        server.send_message(msg)
        server.quit()
        print("Daily report email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

def export_database(table_name):
    """Export database table to CSV."""
    filename = f"{table_name}_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
    response = supabase.table(table_name).select("*").execute()
    
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        # Write headers
        if response.data:
            writer.writerow(response.data[0].keys())
            # Write rows
            for row in response.data:
                writer.writerow(row.values())
    
    return filename

def upload_to_drive(filename, folder_id=None):
    """Upload file to Google Drive."""
    # Decode and save credentials
    credentials_json = base64.b64decode(os.environ.get("GOOGLE_DRIVE_CREDENTIALS")).decode('utf-8')
    credentials_dict = json.loads(credentials_json)
    
    with open('credentials.json', 'w') as f:
        json.dump(credentials_dict, f)
    
    credentials = service_account.Credentials.from_service_account_file(
        'credentials.json', 
        scopes=['https://www.googleapis.com/auth/drive']
    )
    
    drive_service = build('drive', 'v3', credentials=credentials)
    
    file_metadata = {
        'name': filename,
        'parents': [folder_id] if folder_id else []
    }
    
    media = MediaFileUpload(filename, resumable=True)
    
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    
    print(f"File {filename} uploaded to Google Drive with ID: {file.get('id')}")
    
    # Clean up
    os.remove('credentials.json')
    os.remove(filename)

def weekly_backup():
    """Create weekly backups and upload to Google Drive."""
    leads_file = export_database("leads_db")
    orgs_file = export_database("orgs_db")
    
    # Upload to Google Drive
    upload_to_drive(leads_file)
    upload_to_drive(orgs_file)
    
    print("Weekly backup completed!")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "weekly":
        weekly_backup()
    else:
        send_daily_report()
