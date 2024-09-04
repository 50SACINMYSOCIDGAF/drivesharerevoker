import os
import logging
import argparse
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from openpyxl import Workbook

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = 'credentials.json'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Add these constants for rate limiting
BASE_DELAY = 1  # Base delay in seconds
MAX_DELAY = 32  # Maximum delay in seconds
MAX_RETRIES = 5  # Maximum number of retries

def exponential_backoff(func):
    def wrapper(*args, **kwargs):
        delay = BASE_DELAY
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except HttpError as error:
                if error.resp.status in [429, 500, 503]:  # Rate limit error codes
                    if attempt == MAX_RETRIES - 1:
                        raise
                    sleep_time = min(delay * (2 ** attempt), MAX_DELAY)
                    logging.warning(f"Rate limit hit. Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    raise
    return wrapper

@exponential_backoff
def get_service():
    """
    Creates and returns a Google Drive service object using service account credentials.

    Potential issues:
    1. Make sure the service account has the necessary permissions in your Google Workspace.
    2. Ensure the SERVICE_ACCOUNT_FILE is present in the same directory as this script.
    """
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

def is_allbirds_email(email):
    """
    Checks if an email belongs to your domain.

    Note: This function may need to be updated if your domain uses additional email domains.
    """
    return email.lower().endswith('@domain.com') or email.lower().endswith('@ext.domain.com')

@exponential_backoff
def get_shared_files_and_folders(service, limit):
    """
    Retrieves files and folders shared with the service account.

    Potential issues:
    1. The 'sharedWithMe' query might not catch all shared files if the service account
       has been added directly to a shared drive or folder.
    2. This function might hit API quotas if there are many shared files.
    """
    query = "sharedWithMe"
    fields = "nextPageToken, files(id, name, mimeType, owners, permissions, webViewLink)"
    all_items = []
    page_token = None

    while True:
        results = service.files().list(
            q=query,
            fields=fields,
            pageSize=100,
            pageToken=page_token
        ).execute()

        items = results.get('files', [])
        all_items.extend(items)
        logging.info(f"Found {len(items)} shared items")

        if limit and len(all_items) >= limit:
            return all_items[:limit]

        page_token = results.get('nextPageToken')
        if not page_token:
            break

    return all_items

@exponential_backoff
def get_folder_contents(service, folder_id):
    """
    Retrieves the contents of a specific folder.

    Potential issues:
    1. For very large folders, this function might hit API quotas.
    2. It doesn't handle nested folder structures beyond one level.
    """
    query = f"'{folder_id}' in parents"
    fields = "nextPageToken, files(id, name, mimeType, owners, permissions, webViewLink)"
    all_items = []
    page_token = None

    while True:
        results = service.files().list(
            q=query,
            fields=fields,
            pageSize=100,
            pageToken=page_token
        ).execute()

        items = results.get('files', [])
        all_items.extend(items)
        logging.info(f"Found {len(items)} items in folder {folder_id}")

        page_token = results.get('nextPageToken')
        if not page_token:
            break

    return all_items


def process_items_recursively(service, items, processed_ids, limit):
    """
    Recursively processes items, handling both files and folders.

    This function is the core of the recursive search. It processes each item:
    - If it's a file, it's added to the list.
    - If it's a folder, its contents are retrieved and processed recursively.

    Potential issues:
    1. For deeply nested folder structures, this might cause a stack overflow.
    2. It might hit API quotas for large folder structures.
    """
    all_files = []
    for item in items:
        if item['id'] in processed_ids:
            continue
        processed_ids.add(item['id'])

        if item['mimeType'] == 'application/vnd.google-apps.folder':
            folder_contents = get_folder_contents(service, item['id'])
            all_files.extend(process_items_recursively(service, folder_contents, processed_ids, limit))
        else:
            all_files.append(item)

        if limit and len(all_files) >= limit:
            return all_files[:limit]

    return all_files


@exponential_backoff
def process_file(service, file, revoke_permissions):
    """
    Processes a single file, checking for external permissions and optionally revoking them.

    Potential issues:
    1. If a file has many permissions, this might hit API quotas.
    2. Revoking permissions might fail if the service account doesn't have sufficient rights.
    """
    file_id = file['id']
    file_name = file.get('name', 'Unknown')
    file_link = file.get('webViewLink', 'Unknown')

    external_emails = []
    permissions_revoked = False

    for permission in file.get('permissions', []):
        email = permission.get('emailAddress', '')
        if permission.get('type') == 'user' and not is_allbirds_email(email):
            external_emails.append(email)

            if revoke_permissions:
                service.permissions().delete(fileId=file_id, permissionId=permission['id']).execute()
                logging.info(f"Revoked access for file ID: {file_id}, permission ID: {permission['id']}")
                permissions_revoked = True
            else:
                logging.info(
                    f"[DRY RUN] Would revoke access for file ID: {file_id}, permission ID: {permission['id']}")

    return {
        'name': file_name,
        'link': file_link,
        'external_emails': ", ".join(external_emails),
        'permissions_revoked': permissions_revoked
    }


def export_to_spreadsheet(file_data, output_file):
    """
    Exports the processed file data to an Excel spreadsheet.

    Potential issues:
    1. For very large datasets, this might consume a lot of memory.
    2. Ensure the script has write permissions in the directory where the output file will be saved.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Shared Files Report"

    headers = ['File Name', 'File Link', 'External Emails', 'Permissions Revoked']
    ws.append(headers)

    for data in file_data:
        ws.append([data['name'], data['link'], data['external_emails'], data['permissions_revoked']])

    wb.save(output_file)
    logging.info(f"Report exported to {output_file}")


def main(revoke_permissions, limit, output_file):
    """
    Main function that orchestrates the entire process.

    This function ties everything together:
    1. Sets up the Drive service
    2. Retrieves shared files and folders
    3. Processes items recursively
    4. Handles individual files (checking/revoking permissions)
    5. Exports results to a spreadsheet

    Potential issues:
    1. For very large Drive structures, this might take a long time to complete.
    2. It might hit various API quotas (especially if revoking many permissions).
    """
    try:
        service = get_service()

        logging.info(f"Checking up to {limit} files.")
        shared_items = get_shared_files_and_folders(service, limit)
        all_files = process_items_recursively(service, shared_items, set(), limit)
        logging.info(f"Total files found: {len(all_files)}")

        file_data = [process_file(service, file, revoke_permissions) for file in all_files]
        export_to_spreadsheet(file_data, output_file)

    except HttpError as error:
        logging.error(f"An error occurred: {error}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Google Drive Shared Files Checker and Permission Revoker')
    parser.add_argument('--revoke', action='store_true', help='Run in active mode (permissions will be revoked)')
    parser.add_argument('--limit', type=int, default=1000, help='Limit the number of files to check (default: 1000)')
    parser.add_argument('--output', type=str, default='report.xlsx',
                        help='Output file for the report (default: report.xlsx)')
    args = parser.parse_args()

    if args.revoke:
        logging.warning("Running in ACTIVE mode. Permissions will be revoked.")
    else:
        logging.info("Running in DRY RUN mode. No permissions will be revoked.")

    main(revoke_permissions=args.revoke, limit=args.limit, output_file=args.output)