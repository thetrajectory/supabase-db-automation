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

def get_new_rows_today(table_name):
    """Get count of new rows added today (assuming there's a created_at column)."""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    try:
        # Try with created_at column first
        response = supabase.table(table_name).select("count", count="exact").gte("created_at", today).execute()
        return response.count
    except Exception:
        try:
            # Try with a different timestamp column if created_at doesn't exist
            response = supabase.table(table_name).select("count", count="exact").gte("timestamp", today).execute()
            return response.count
        except Exception:
            print(f"Warning: Could not find timestamp column in {table_name}. Returning 0.")
            return 0

def get_request_count(table_name):
    """
    Get the number of fetch/get requests made on a specific table.
    
    This function tries two approaches:
    1. Query PostgreSQL stats views if available
    2. Use a tracking table if you have one
    
    If neither works, we'll need to create our own tracking system.
    """
    try:
        # Approach 1: Try to query PostgreSQL stats views
        # This SQL query tries to get request statistics from pg_stat_user_tables
        sql = f"""
        SELECT n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, n_live_tup, n_dead_tup, seq_scan, idx_scan  
        FROM pg_stat_user_tables 
        WHERE relname = '{table_name}'
        """
        
        # Execute raw SQL through Supabase's RPC call
        response = supabase.rpc('pg_stat_query', {'query_sql': sql}).execute()
        
        if response.data:
            stats = response.data[0]
            # idx_scan is generally a good proxy for API fetches/gets
            return stats.get('idx_scan', 0) + stats.get('seq_scan', 0)
    except Exception as e:
        print(f"Could not get PostgreSQL stats: {e}")
    
    try:
        # Approach 2: Try to query a custom request_logs table if you have one
        # This assumes you might have set up a custom logging table
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        response = supabase.table('request_logs').select("count", count="exact").eq("table_name", table_name).gte("timestamp", today).execute()
        return response.count
    except Exception as e:
        print(f"Could not query request logs: {e}")
    
    # If we get here, we couldn't retrieve request counts
    # Return a placeholder and suggest creating a tracking system
    print(f"Cannot retrieve request count for {table_name}. Consider setting up a tracking system.")
    return "N/A (tracking not available)"

def send_daily_report():
    """Generate and send daily report via email."""
    # Get basic stats
    leads_total = get_total_rows("leads_db")
    orgs_total = get_total_rows("orgs_db")
    new_leads_today = get_new_rows_today("leads_db")
    new_orgs_today = get_new_rows_today("orgs_db")
    
    # Get request counts
    leads_requests = get_request_count("leads_db")
    orgs_requests = get_request_count("orgs_db")
    
    # Create email
    msg = MIMEMultipart()
    msg["From"] = gmail_user
    msg["To"] = report_recipient
    msg["Subject"] = f"Supabase Daily Report - {datetime.datetime.now().strftime('%Y-%m-%d')}"
    
    body = f"""
    <html>
      <body>
        <h2>Supabase Daily Report</h2>
        
        <h3>Leads Database Report:</h3>
        <ul>
          <li>Total Rows: {leads_total}</li>
          <li>New Rows Added Today: {new_leads_today}</li>
          <li>Fetch/Get Requests Today: {leads_requests}</li>
        </ul>
        
        <h3>Organizations Database Report:</h3>
        <ul>
          <li>Total Rows: {orgs_total}</li>
          <li>New Rows Added Today: {new_orgs_today}</li>
          <li>Fetch/Get Requests Today: {orgs_requests}</li>
        </ul>
        
        <p>This report was automatically generated at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC.</p>
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

def export_database_paginated(table_name):
    """Export database table to CSV using pagination to avoid timeouts."""
    filename = f"{table_name}_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
    
    # First, try to get a single row to determine the columns
    try:
        first_row = supabase.table(table_name).select("*").limit(1).execute()
        if not first_row.data:
            print(f"Warning: No data found in table {table_name}")
            # Create empty file with just headers
            with open(filename, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["id"])  # Minimal header
            return filename
        
        headers = list(first_row.data[0].keys())
        
        # Open file and write headers
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            
            # Use pagination to fetch all rows
            page_size = 500  # Reduced from 1000 to avoid timeouts
            page = 0
            more_data = True
            
            print(f"Exporting {table_name} with pagination (page size: {page_size})...")
            
            while more_data:
                print(f"Fetching page {page} of {table_name}...")
                
                # PostgreSQL uses OFFSET and LIMIT for pagination
                response = supabase.table(table_name) \
                                 .select("*") \
                                 .range(page * page_size, (page + 1) * page_size - 1) \
                                 .execute()
                
                rows = response.data
                if not rows:
                    more_data = False
                else:
                    # Write this batch of rows
                    for row in rows:
                        # Ensure all values are in the same order as headers
                        row_values = [row.get(header, "") for header in headers]
                        writer.writerow(row_values)
                    
                    # Move to next page
                    page += 1
                    
                    # If we got fewer rows than requested, we're done
                    if len(rows) < page_size:
                        more_data = False
        
        print(f"Exported {table_name} successfully!")
        return filename
        
    except Exception as e:
        print(f"Error exporting {table_name}: {e}")
        raise

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
    print("Starting weekly backup...")
    
    try:
        print("Exporting leads_db...")
        leads_file = export_database_paginated("leads_db")
        print(f"Successfully exported leads_db to {leads_file}")
        
        print("Exporting orgs_db...")
        orgs_file = export_database_paginated("orgs_db")
        print(f"Successfully exported orgs_db to {orgs_file}")
        
        # Upload to Google Drive
        print("Uploading leads_db to Google Drive...")
        upload_to_drive(leads_file)
        
        print("Uploading orgs_db to Google Drive...")
        upload_to_drive(orgs_file)
        
        print("Weekly backup completed successfully!")
    except Exception as e:
        print(f"Weekly backup failed: {e}")
        raise

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "weekly":
        weekly_backup()
    else:
        send_daily_report()
