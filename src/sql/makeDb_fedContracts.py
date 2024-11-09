import os
import sqlite3

def create_table(table_name, columns):
    cursor = conn.cursor()
    # Create a dynamic SQL query to create a table
    column_defs = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs})"
    # Execute the query
    cursor.execute(query)
    # Commit and close
    conn.commit()
    print(table_name)

conn = sqlite3.connect('fed_contracts.db')

#Initialize a data table to store temporal representations of the raster data for the tile
#FEATURE_ID,FEATURE_NAME,FEATURE_CLASS,STATE_ALPHA,STATE_NUMERIC,COUNTY_NAME,COUNTY_NUMERIC,PRIMARY_LAT_DMS,PRIM_LONG_DMS,PRIM_LAT_DEC,PRIM_LONG_DEC,SOURCE_LAT_DMS,SOURCE_LONG_DMS,SOURCE_LAT_DEC,SOURCE_LONG_DEC,ELEV_IN_M,ELEV_IN_FT,MAP_NAME,DATE_CREATED,DATE_EDITED
create_table('dot', {
    'contractId': 'TEXT PRIMARY KEY', 'zipcode': 'TEXT',  'stateCode': 'TEXT', 'permalink': 'TEXT', 'description': 'TEXT',
    'actionDate': 'TEXT', 'startDate': 'TEXT', 'endDate': 'TEXT', 'naicsDesc': 'TEXT', 'naicsCode': 'TEXT', 'recipient': 'TEXT',
    'fedActionObligation': 'TEXT', 'totDolar': 'TEXT', 'fundingSubAgency': 'TEXT', 'zipcode_lat': 'FLOAT', 'zipcode_lon': 'FLOAT'
    })
conn.close()
