import datetime
import re
import pandas as pd
import pyodbc
import sqlalchemy
import os
from pandas import DataFrame 
import sqlalchemy
from sqlalchemy import create_engine
from models.exceptions.DataBaseError import DataBaseError
from constants.constants import Table

# ----------------------------------------------- #
# -------------------- General ------------------ #
# ----------------------------------------------- #
def extract_schema_and_table(table_name: str) -> tuple[str, str]:
    match = re.match(r'\[([^]]+)\]\.\[([^]]+)\]', table_name)
    if match:
        schema = match.group(1)
        table = match.group(2)
        return schema, table
    else:
        return None, None
    
def validate_table_name(table_name: str) -> bool:
    schema, table = extract_schema_and_table(table_name)
    return not(schema == None and table == None)

def fill_nulls_on_premise(data:DataFrame, table: str) -> DataFrame:
    column_typos:DataFrame = get_column_typos_on_premise(table)
    
    data_columns = data.columns
    db_columns = column_typos['COLUMN_NAME'].unique()

    for col in db_columns:
        db_field = column_typos[column_typos['COLUMN_NAME']==col]
        if not db_field.empty:
            typo = db_field.iloc[0]['DATA_TYPE']
            nullable = db_field.iloc[0]['IS_NULLABLE']
            if nullable == "NO":
                if typo=='int':
                    if col.lower() in map(str.lower, data_columns):
                        data_column = next(column for column in data_columns if column.lower() == col.lower())
                        data[data_column] = data[data_column].fillna(value=0)
                    else:
                        data[col] = 0
                elif typo=='decimal':
                    if col.lower() in map(str.lower, data_columns):
                        data_column = next(column for column in data_columns if column.lower() == col.lower())
                        data[data_column] = data[data_column].fillna(value=0.0)
                    else:
                        data[col] = 0.0
                elif typo=='date':
                    default_date = pd.to_datetime('01/01/1900 0:00').normalize()
                    if col.lower() in map(str.lower, data_columns):
                        data_column = next(column for column in data_columns if column.lower() == col.lower())
                        data[data_column] = data[data_column].fillna(value=default_date)
                    else:
                        data[col] = default_date
                elif typo=='nvarchar' or typo=='varchar' or typo=='char':
                    if col.lower() in map(str.lower, data_columns):
                        data_column = next(column for column in data_columns if column.lower() == col.lower())
                        data[data_column] = data[data_column].fillna(value="")
                    else:
                        data[col] = ""
    return data

def insert(data: DataFrame, table_name: str) -> None:
    try:
        connection_string = os.environ["AZURE_SQL_CONNECTION_STRING"]
        connection_url = sqlalchemy.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url, fast_executemany=True)
        conn = engine.connect()
        schema, table = extract_schema_and_table(table_name)
        data.to_sql(table, schema = schema, con = conn, if_exists='append', index=False, chunksize=1000)
        conn.close()
        engine.dispose()
        # TODO: Remove when on-premise syncronization is gone
        if os.environ["ON_PREMISE_SQL_ACTIVE_FLAG"] == 'true':
            insert_on_premise(data, table_name)
    except Exception as e: 
        if 'conn' in locals():
            conn.close()
        if 'engine' in locals():
            engine.dispose()
        raise DataBaseError('DataBaseError', str(e))

def insert_on_premise(data: DataFrame, table_name: str) -> None:
    try:
        connection_string = os.environ["ON_PREMISE_SQL_CONNECTION_STRING"]
        connection_url = sqlalchemy.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url, fast_executemany=True)
        conn = engine.connect()
        schema, table = extract_schema_and_table(table_name)
        data = fill_nulls_on_premise(data, table_name)
        data.to_sql(table, schema = schema, con = conn, if_exists='append', index=False, chunksize=1000)
        conn.close()
        engine.dispose()
    except Exception as e: 
        if 'conn' in locals():
            conn.close()
        if 'engine' in locals():
            engine.dispose()
        raise DataBaseError('DataBaseError', str(e))

def delete(id:str, table: str) -> None:
    try:    
        if not validate_table_name(table):
            raise Exception()
        connection_string = os.environ["AZURE_SQL_CONNECTION_STRING"]
        conn = pyodbc.connect(connection_string)
        cursor=conn.cursor()
        cursor.execute(f"DELETE FROM {table} WHERE ID=?", id)
        conn.commit()
        conn.close()
        # TODO: Remove when on-premise syncronization is gone
        if os.environ["ON_PREMISE_SQL_ACTIVE_FLAG"] == 'true':
            delete_on_premise(id, table)
    except Exception as e: 
        if 'conn' in locals():
            conn.close()
        raise DataBaseError('DataBaseError', str(e))
    
def delete_on_premise(id:str, table: str) -> None:
    try:    
        if not validate_table_name(table):
            raise Exception()
        connection_string = os.environ["ON_PREMISE_SQL_CONNECTION_STRING"]
        conn = pyodbc.connect(connection_string)
        cursor=conn.cursor()
        cursor.execute(f"DELETE FROM {table} WHERE ID=?", id)
        conn.commit()
        conn.close()
    except Exception as e: 
        if 'conn' in locals():
            conn.close()
        raise DataBaseError('DataBaseError', str(e))
            
def test_on_premise_connection() -> None:
    try: 
        connection_string = os.environ["ON_PREMISE_SQL_CONNECTION_STRING"]
        conn = pyodbc.connect(connection_string)
        cursor=conn.cursor()
        cursor.execute("SELECT 1 AS NUMBER")
        number = cursor.fetchall()[0].NUMBER
        conn.commit()
        conn.close()
    except Exception as e: 
        if 'conn' in locals():
            conn.close()
        raise DataBaseError('DataBaseError', str(e))

def save_execution_log(template_type: str, id: int, action: str) -> None:
    try:
        log = [{'TEMPLATE': template_type, 'ID': id, 'ACTION': action, 'SYNC_FLAG': False, 'TIMESTAMP': datetime.datetime.now()}]  
        logs = pd.DataFrame.from_records(log,index=['1']) 
        insert(logs, Table.EXECUTION_LOGS)
    except Exception as e: 
        raise DataBaseError('DataBaseError', str(e))

def get_next_id():
    try: 
        connection_string = os.environ["AZURE_SQL_CONNECTION_STRING"]
        conn = pyodbc.connect(connection_string)
        cursor=conn.cursor()
        cursor.execute("SELECT NEXT VALUE FOR [DWH].[ID_SEQ]")
        set_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        return set_id
    except Exception as e: 
        if 'conn' in locals():
            conn.close()
        raise DataBaseError('DataBaseError', str(e))
    
def select(id: str) -> DataFrame:
    try:    
        connection_string = os.environ["AZURE_SQL_CONNECTION_STRING"]
        conn = pyodbc.connect(connection_string)
        sql =   """
                    SELECT  *
                    FROM [SCHEMA].[TABLE]
                    WHERE ID=?    
                """
        table = pd.read_sql(sql,conn, params=(id))
        conn.commit()
        conn.close()
        return table
    except Exception as e: 
        if 'conn' in locals():
            conn.close()
        raise DataBaseError('DataBaseError', str(e))

def get_column_typos(table:str) -> DataFrame:
    connection_string = os.environ["AZURE_SQL_CONNECTION_STRING"]
    conn = pyodbc.connect(connection_string)
    match = re.match(r'\[([^]]+)\]\.\[([^]]+)\]', table)
    if match:
        schema = match.group(1)
        table = match.group(2)
        sql =   """
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;
                """
        data_validation_rules = pd.read_sql(sql,conn, params=(schema,table))
        conn.commit()
    conn.close()
    return data_validation_rules

def get_column_typos_on_premise(table:str) -> DataFrame:
    connection_string = os.environ["ON_PREMISE_SQL_CONNECTION_STRING"]
    conn = pyodbc.connect(connection_string)
    match = re.match(r'\[([^]]+)\]\.\[([^]]+)\]', table)
    if match:
        schema = match.group(1)
        table = match.group(2)
        sql =   """
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;
                """
        data_validation_rules = pd.read_sql(sql,conn, params=(schema,table))
        conn.commit()
    conn.close()
    return data_validation_rules