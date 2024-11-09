import sqlite3
import pandas as pd
import json
import os
from os import listdir
from os.path import isfile, join
import csv
######################################################################################
def format_zipcode(zipcode,targetLen):
	zipcode=str(zipcode)
	#
	zcLen = len(zipcode)
	if zcLen != targetLen:
		if zcLen <=5:
			lenDelta = abs(zcLen-targetLen)
			chars_ = []
			for i in range(lenDelta):
				chars_.append(str(0))
			for c in zipcode:
				chars_.append(c)
			return("".join(chars_))
		elif zcLen > 5:
			zipcode=zipcode[0:int(len(zipcode)-4)]
			lenDelta = abs(len(zipcode)-targetLen)
			chars_ = []
			for i in range(lenDelta):
				chars_.append(str(0))
			for c in zipcode:
				chars_.append(c)
			return("".join(chars_))
		return(zipcode)
	
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
######################################################################################
conn_uscbGaz  = sqlite3.connect('uscb_gazetter.db')  # Replace 'example.db' with your database name
cursor_uscbGaz = conn_uscbGaz.cursor()
#CSAFP	GEOID	NAME	CBSA_TYPE	ALAND	AWATER	ALAND_SQMI	AWATER_SQMI	INTPTLAT	INTPTLONG  
tableId = str('zcta_2021')
query = f"SELECT * FROM {tableId}"
cursor_uscbGaz.execute(query)
#rows = cursor.fetchall()

naicsRef_ = {}
with open("%s%s" % (r"resources/naics/","naicsCodes.csv"), 'r', newline='') as csvfile:
    csv_reader = csv.reader(csvfile)
    for row in csv_reader:
        naicsRef_[str(row[0])] = str(row[1])

# Connect to the SQLite database
conn = sqlite3.connect('fed_contracts.db')
cursor = conn.cursor()

# List of tables (replace with your own feature class names if needed)
#selectFeatureClasses = ["Airport", "Hospital", "School"]
headerRef_json = json.load(open("%s%s" % (r"resources/","headerRef.json")))
######################################################################################
files_ = [f for f in listdir(r"data/") if isfile(join(r"data/", f))]
#for fName in run["fed_contracts"]["files"]:
for fName in ['FY23_DOT_CON.csv']:
	agencyCode = fName.split("_")[1]
	if fName in files_:

		df = pd.read_csv(
			str("%s%s" % (r"data/",fName)
		),encoding="utf-8", low_memory=False)

		#for featureClassName in selectFeatureClasses:
		#	split_featureClassName = featureClassName.split(" ")
		#	tableId_featureClass = str("_".join(split_featureClassName).lower())
			
		# Filter the DataFrame for the current feature class
		#df_filtered = df_usgsFeatures[df_usgsFeatures["FEATURE_CLASS"] == featureClassName]

		tableId = 'dot'

		#Create the table for the feature class
		create_table(tableId, {
			'contractId': 'TEXT PRIMARY KEY', 'zipcode': 'TEXT',  'stateCode': 'TEXT', 'permalink': 'TEXT', 'description': 'TEXT',
			'actionDate': 'TEXT', 'startDate': 'TEXT', 'endDate': 'TEXT', 'naicsDesc': 'TEXT', 'naicsCode': 'TEXT', 'recipient': 'TEXT',
			'fedActionObligation': 'TEXT', 'totDolar': 'TEXT', 'fundingSubAgency': 'TEXT', 'zipcode_lat': 'FLOAT', 'zipcode_lon': 'FLOAT'
		})

		for _, row in df.iterrows():
			zipcode = zipcode = format_zipcode(row[headerRef_json["zipcode"][agencyCode]],5)

			cursor_uscbGaz.execute("SELECT intplat, intplong FROM zcta_2021 WHERE geoid = ?", (zipcode,))
			result = cursor_uscbGaz.fetchone()

			if result:
				intptlat, intptlong = result
				zcta_coords = [intptlong, intptlat]

				cursor.execute(f"""
				INSERT OR IGNORE INTO {tableId} (
					'contractId', 'zipcode',  'stateCode', 'permalink', 'description',
					'actionDate', 'startDate', 'endDate', 'naicsDesc', 'naicsCode', 
					'recipient', 'fedActionObligation', 'totDolar', 'fundingSubAgency',
					'zipcode_lat', 'zipcode_lon'
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				""", (
					row[headerRef_json["contractId"][agencyCode]],
					zipcode,
					row[headerRef_json["stateCode"][agencyCode]],
					row[headerRef_json["permalink"][agencyCode]],
					row[headerRef_json["description"][agencyCode]],
					row[headerRef_json["actionDate"][agencyCode]],
					row[headerRef_json["startDate"][agencyCode]],
					row[headerRef_json["endDate"][agencyCode]],
					row[headerRef_json["naicsDesc"][agencyCode]],
					row[headerRef_json["naicsCode"][agencyCode]],
					row[headerRef_json["recipient"][agencyCode]],
					row[headerRef_json["fedActionObligation"][agencyCode]],
					row[headerRef_json["totDolar"][agencyCode]],
					row[headerRef_json["fundingSubAgency"][agencyCode]],
					intptlat, intptlong
				))
######################################################################################
# Commit the changes and close the connection
conn.commit()
conn.close()

print('DB Connection Closed')
