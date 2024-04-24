from repositories.storage_repository import download_blob_to_stream
import pandas as pd
from pandas import DataFrame 
import os
import io
import re

def download_excel(file_name):
    container_name = os.environ["AZURE_STORAGE_CONTAINER_NAME_DROPFILES"];
    blob_data = download_blob_to_stream(container_name, blob_name=file_name)
    blob_data.name = file_name
    return blob_data

def get_excel_tabs(excel: io.BytesIO)->list[str]:
    return pd.ExcelFile(excel).sheet_names

def ignore_tabs_filter(tabs:list[str], ignore_tabs:list[str] ) -> list[str]:
    filtered_tabs = [tab for tab in tabs if not any(re.search(ignore_tab, tab) for ignore_tab in ignore_tabs)]
    return filtered_tabs

def ignore_empty_tabs_filter(tabs:list[str], excel: io.BytesIO) -> list[str]:
    pass

def parse_tab(excel:io.BytesIO)-> DataFrame:
    df = pd.read_excel(excel, sheet_name='tab', engine='openpyxl', usecols='A:B', skiprows=1, nrows=2, header=None, names=['Tab','Values'])
    # Sanitize headers
    df = df.transpose()
    df.columns = df.iloc[0]
    df = df[1:]
    df = df.reset_index(drop=True)
    return df

# ----------------------------------------------- #
# --------------- Excel Result File ------------- #
# ----------------------------------------------- #

def create_excel_results(template, file_name, status, user) -> io.BytesIO:
    excel_file = io.BytesIO()
    writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')
    
    file_data = DataFrame({'Template':[template], 'Filename':[file_name]})
    if not file_data.empty:
        file_data.to_excel(writer, sheet_name='Summary', index=False, startrow=0, startcol=0)

    file_summary = DataFrame({'User':[user], 'Status':[status]})
    if not file_summary.empty:
        file_summary.to_excel(writer, sheet_name='Summary', index=False, startrow=3, startcol=0)

    # Format excel file
    workbook = writer.book
    summary_worksheets = [sheet.name for sheet in writer.sheets.values() if re.search(r'Summary', sheet.name, re.IGNORECASE)]

    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top',
        'fg_color': '#D7E4BC',
        'bottom': 2,
        'right':1,
        'bottom_color': '#425321',
        'right_color':'#B2CB7F',
    })
    
    for col_num, value in enumerate(file_data.columns.values):
        summary_worksheets[0].write(0, col_num, value, header_format)
    for col_num, value in enumerate(file_summary.columns.values):
        summary_worksheets[0].write(3, col_num, value, header_format)
    summary_worksheets[0].autofit()

    # Save excel file
    writer.close()
    # Reset the file pointer to the beginning of the in-memory file
    excel_file.seek(0)

    return excel_file
