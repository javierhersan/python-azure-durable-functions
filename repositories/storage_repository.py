from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError
import io
import os

def create_blob_container(container_name):
    try:
        connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"];
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.create_container(name=container_name)
    except ResourceExistsError:
        print('A container with this name already exists')

def upload_blob_stream(container_name: str, filename, content):
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"];
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    blob_client.upload_blob(content, blob_type="BlockBlob", overwrite=True)

def upload_blob_tags(container_name: str):
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"];
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container=container_name)
    sample_tags = {"Content": "image", "Date": "2022-01-01"}
    with open(file=os.path.join('filepath', 'filename'), mode="rb") as data:
        blob_client = container_client.upload_blob(name="sample-blob.txt", data=data, tags=sample_tags)

def download_blob_to_stream(container_name, blob_name):
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"];
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    stream = io.BytesIO()
    data = blob_client.download_blob().readall()
    stream.write(data)
    stream.seek(0)
    return stream