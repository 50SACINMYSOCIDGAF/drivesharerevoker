import datetime
import logging
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up credentials
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'path/to/service_account.json'
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Build the Drive API client
drive_service = build('drive', 'v3', credentials=credentials)


def get_shared_files():
    query = "sharedWithNonDomainUsers = true"
    fields = "nextPageToken, files(id, name, owners, permissions, sharingUser, shared)"
    try:
        results = drive_service.files().list(q=query, fields=fields).execute()
        return results.get('files', [])
    except HttpError as error:
        logging.error(f"An error occurred while fetching shared files: {error}")
        return []


def is_organization_email(email):
    return email.lower().endswith('@YOUR_DOMAIN_HERE')


def revoke_external_access(file_id, permission_id):
    try:
        drive_service.permissions().delete(fileId=file_id, permissionId=permission_id).execute()
        logging.info(f"Revoked access for file ID: {file_id}, permission ID: {permission_id}")
    except HttpError as error:
        logging.error(f"Error revoking access for file ID: {file_id}, permission ID: {permission_id}. Error: {error}")


def process_file_permissions(file):
    file_owner = file['owners'][0]['emailAddress'] if file.get('owners') else None

    if not is_organization_email(file_owner):
        logging.warning(f"Skipping file {file['id']} - owner is not from ORGANIZATION")
        return

    three_months_ago = datetime.datetime.utcnow() - datetime.timedelta(days=90)

    for permission in file.get('permissions', []):
        try:
            if permission.get('type') == 'user' and not is_organization_email(permission.get('emailAddress', '')):
                # Check when this permission was created
                perm_details = drive_service.permissions().get(
                    fileId=file['id'],
                    permissionId=permission['id'],
                    fields="createdTime"
                ).execute()

                created_time = datetime.datetime.strptime(perm_details['createdTime'], "%Y-%m-%dT%H:%M:%S.%fZ")

                if created_time < three_months_ago:
                    revoke_external_access(file['id'], permission['id'])
        except HttpError as error:
            logging.error(f"Error processing permission {permission['id']} for file {file['id']}: {error}")


def main():
    shared_files = get_shared_files()
    logging.info(f"Found {len(shared_files)} shared files")

    for file in shared_files:
        try:
            process_file_permissions(file)
        except Exception as e:
            logging.error(f"Error processing file {file['id']}: {e}")


if __name__ == '__main__':
    main()