import streamlit as st
import os
import zipfile
import tempfile
from google.cloud import storage
from google.oauth2 import service_account

def create_bucket_if_not_exists(bucket_name, credentials):
    """
    Create a GCS bucket if it does not exist.
    """
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name)
        st.success(f"Bucket '{bucket_name}' created.")
    else:
        st.info(f"Bucket '{bucket_name}' already exists.")
    return bucket

def upload_files_to_gcs(bucket_name, uploaded_files, destination_folder, credentials):
    """
    Uploads files to the specified GCS bucket and folder.
    """
    bucket = create_bucket_if_not_exists(bucket_name, credentials)

    for uploaded_file in uploaded_files:
        filename = uploaded_file.name
        file_extension = os.path.splitext(filename)[1].lower()
        if file_extension == '.zip':
            with tempfile.TemporaryDirectory() as tmpdirname:
                try:
                    # Save the uploaded zip file to a temporary file
                    temp_zip_path = os.path.join(tmpdirname, filename)
                    with open(temp_zip_path, 'wb') as temp_zip_file:
                        temp_zip_file.write(uploaded_file.getbuffer())

                    # Unzip the file
                    with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                        zip_ref.extractall(tmpdirname)

                    # Walk through the extracted files and upload them
                    for root, dirs, files in os.walk(tmpdirname):
                        for file in files:
                            local_path = os.path.join(root, file)
                            relative_path = os.path.relpath(local_path, tmpdirname)
                            blob_path = os.path.join(destination_folder, os.path.splitext(filename)[0], relative_path)

                            blob = bucket.blob(blob_path)
                            blob.upload_from_filename(local_path)
                            st.write(f"Uploaded '{blob_path}' to GCS bucket '{bucket_name}'")
                except Exception as e:
                    st.error(f"Error processing zip file '{filename}': {str(e)}")
        else:
            # Save the uploaded file to a temporary file
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(uploaded_file.getbuffer())
                temp_file.flush()
                blob_path = os.path.join(destination_folder, filename)
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(temp_file.name)
                st.write(f"Uploaded '{blob_path}' to GCS bucket '{bucket_name}'")
                os.unlink(temp_file.name)

def main():
    st.title("Upload Files to Google Cloud Storage")

    # Load service account credentials from st.secrets
    credentials_info = st.secrets["service_account"]
    if not credentials_info:
        st.error("Service account credentials not found in secrets.")
        st.stop()

    # Ensure the private key is properly formatted
    if isinstance(credentials_info['private_key'], str):
        credentials_info['private_key'] = credentials_info['private_key'].replace('\\n', '\n')

    credentials = service_account.Credentials.from_service_account_info(credentials_info)

    bucket_name = st.text_input("Enter GCS Bucket Name:")

    destination_folder = st.text_input("Enter Destination Folder in Bucket (optional):", value="")

    uploaded_files = st.file_uploader("Choose files to upload", accept_multiple_files=True)

    if st.button("Upload"):
        if not bucket_name:
            st.error("Please enter a bucket name.")
        elif not uploaded_files:
            st.error("Please select files to upload.")
        else:
            with st.spinner("Uploading files..."):
                upload_files_to_gcs(bucket_name, uploaded_files, destination_folder, credentials)
            st.success("File upload complete.")

if __name__ == "__main__":
    main()
