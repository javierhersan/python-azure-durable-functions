import pytest
import os
from io import BytesIO


def test_insert_epi_header():
    # > Initializations and Mocks
    # >>> Load local.settings.json
    os.environ.clear()
    os.environ.update({
        "AZURE_STORAGE_CONNECTION_STRING": "azure_stg_conn_str",
        "AZURE_STORAGE_CONTAINER_NAME_DROPFILES": "dropfiles",
        "AZURE_SQL_CONNECTION_STRING": "azure_sql_conn_str",
        "ON_PREMISE_SQL_CONNECTION_STRING": "on_prem_sql_conn_str",
    })
    # >>> Create mocks with Monkey Patching
    def download_blob_to_stream(container_name, blob_name):
        file_path = os.path.abspath('tests/files/test.xlsx')
        with open(file_path, 'rb') as file:
            bytes_data = file.read()
            return BytesIO(bytes_data)
    import repositories.storage_repository
    repositories.storage_repository.download_blob_to_stream = download_blob_to_stream
    
    # >>> Import functions to test after Monkey Patching (It's needed to make it work)
    from functions.function import func

    # Output
    func_output = func('input')

    # Expected Output

    # Comparisson
    assert True