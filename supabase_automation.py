# supabase_automation.py
import os
import json
import base64
import smtplib
import datetime
import csv
import sys
import time
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
        </ul>
        
        <h3>Organizations Database Report:</h3>
        <ul>
          <li>Total Rows: {orgs_total}</li>
          <li>New Rows Added Today: {new_orgs_today}</li>
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
    """Export database table to CSV using aggressive pagination to avoid timeouts."""
    filename = f"{table_name}_{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
    
    # First, try to get columns via a single row
    try:
        print(f"Determining structure of {table_name}...")
        
        # Try to get column names from a single row
        first_row = supabase.table(table_name).select("*").limit(1).execute()
        
        if not first_row.data:
            print(f"Warning: No data found in table {table_name}")
            # Create empty file with just headers
            with open(filename, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["id"])  # Minimal header
            return filename
        
        headers = list(first_row.data[0].keys())
        
        # Get total count for progress tracking
        total_count = get_total_rows(table_name)
        print(f"Total rows to export from {table_name}: {total_count}")
        
        # Open file and write headers
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            
            # Use very small page size for aggressive pagination
            page_size = 100  # Significantly reduced from 500
            page = 0
            rows_exported = 0
            more_data = True
            
            print(f"Exporting {table_name} with aggressive pagination (page size: {page_size})...")
            
            # Try to find a primary key or id field
            id_field = "id"  # Default
            if "id" not in headers:
                # Look for other common primary key names
                for possible_id in ["uuid", "primary_key", "key", headers[0]]:
                    if possible_id in headers:
                        id_field = possible_id
                        break
            
            print(f"Using {id_field} as primary key for pagination")
            
            # Use cursor-based pagination instead of offset pagination
            last_id = None
            
            while more_data and rows_exported < total_count:
                try:
                    print(f"Fetching batch {page} of {table_name} (rows {rows_exported}/{total_count})...")
                    
                    # Build query
                    query = supabase.table(table_name).select("*").limit(page_size)
                    
                    # Add cursor condition if we have a last_id
                    if last_id is not None:
                        query = query.gt(id_field, last_id)
                    
                    # Order by the id field for consistent pagination
                    query = query.order(id_field, desc=False)
                    
                    # Execute with timeout handling
                    response = query.execute()
                    
                    rows = response.data
                    if not rows:
                        more_data = False
                        print("No more data found.")
                    else:
                        # Write this batch of rows
                        for row in rows:
                            # Ensure all values are in the same order as headers
                            row_values = [row.get(header, "") for header in headers]
                            writer.writerow(row_values)
                        
                        # Keep track of the last ID for cursor pagination
                        last_id = rows[-1].get(id_field)
                        
                        # Update counters
                        rows_exported += len(rows)
                        page += 1
                        
                        # Progress update
                        progress_percent = (rows_exported / total_count) * 100
                        print(f"Progress: {rows_exported}/{total_count} rows ({progress_percent:.1f}%)")
                        
                        # Add a small delay between requests to reduce database load
                        if more_data:
                            print("Pausing briefly to avoid database overload...")
                            time.sleep(1)  # 1 second delay between batches
                
                except Exception as e:
                    # If an individual batch fails, log it but continue with the next one
                    print(f"Error fetching batch {page}: {e}")
                    
                    # If we've already got some data, try to continue with the next batch
                    if rows_exported > 0:
                        page += 1
                        if last_id is not None:
                            print(f"Continuing from ID: {last_id}")
                        else:
                            # Without a valid cursor, we need to skip ahead
                            # This is not ideal but at least gets some data
                            last_id = f"approximate_batch_{page}"
                            print(f"Cannot continue properly, attempting to skip ahead.")
                    else:
                        # If we haven't got any data yet, this is a fatal error
                        raise
        
        print(f"Exported {rows_exported} rows from {table_name} successfully!")
        return filename
        
    except Exception as e:
        print(f"Error exporting {table_name}: {e}")
        
        # If we have a partial file, note that in the filename
        if os.path.exists(filename):
            partial_filename = f"{table_name}_{datetime.datetime.now().strftime('%Y-%m-%d')}_PARTIAL.csv"
            os.rename(filename, partial_filename)
            print(f"Saved partial data to {partial_filename}")
            return partial_filename
            
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
    
    # Get the folder ID from environment variable
    folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")
    
    try:
        print("Exporting leads_db...")
        leads_file = export_database_paginated("leads_db")
        print(f"Successfully exported leads_db to {leads_file}")
        
        print("Exporting orgs_db...")
        orgs_file = export_database_paginated("orgs_db")
        print(f"Successfully exported orgs_db to {orgs_file}")
        
        # Upload to Google Drive with the specified folder ID
        print("Uploading leads_db to Google Drive...")
        upload_to_drive(leads_file, folder_id)
        
        print("Uploading orgs_db to Google Drive...")
        upload_to_drive(orgs_file, folder_id)
        
        print("Weekly backup completed successfully!")
    except Exception as e:
        print(f"Weekly backup failed: {e}")
        raise

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "weekly":
        weekly_backup()
    else:
        send_daily_report()
