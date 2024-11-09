import os
import sqlite3
import pandas as pd
import sqlite3
import time

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

conn = sqlite3.connect('uscb_gazetter.db')

create_table('cbsa_2021', {
    'geoid': 'TEXT PRIMARY KEY', 'csafp': 'TEXT', 'name': 'TEXT', 'cbsa_type': 'TEXT', 'aland': 'INTEGER', 'awater': 'INTEGER',
    'aland_sqmi': 'INTEGER', 'awater_sqmi':'INTEGER', 'intplat': 'FLOAT', 'intplong':'FLOAT'
    })

conn.close()

print('CREATED ', 'cbsa_2021')
time.sleep(1)
conn = sqlite3.connect('uscb_gazetter.db')

'''GEOID	ALAND	AWATER	ALAND_SQMI	AWATER_SQMI	INTPTLAT	INTPTLONG                                                                                                                                  
00601	166848592	798613	64.421	0.308	18.180555	-66.749961    '''

create_table('zcta_2021', {
    'geoid': 'TEXT PRIMARY KEY','aland': 'INTEGER', 'awater': 'INTEGER', 'aland_sqmi': 'INTEGER', 'awater_sqmi': 'INTEGER',
    'intplat': 'INTEGER', 'intplong': 'INTEGER'
    })

conn.close()
print('CREATED ', 'zcta_2021')
