#####################################################################################
import numpy as np
import csv
import math
import pandas as pd
import time
import os
from os import listdir
from os.path import isfile, join
import json
import re
import fiona
import sqlite3

from shapely.geometry import shape, Point, Point, MultiPolygon, Polygon
import geopandas as gpd
#####################################################################################
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

def get_gaz(labels,fname):
	gaz_zcta = {}
	for l in labels:
		gaz_zcta[l] = []
	with open(str("%s%s" % (r"resources/gaz/",fname)),encoding="utf-8") as read_gaz:
		gaz_lines = read_gaz.readlines()
		headers_ = list(map(lambda h: str(h).strip(),gaz_lines[0].split("	")))
		#print(headers_)
		idx_ = []
		for l in labels:
			idx_.append(headers_.index(l))
		for i in range(1,len(gaz_lines),1):
			gaz_line = gaz_lines[i].split("	")
			for l,idx in zip(labels,idx_):
				gaz_zcta[l].append(str(gaz_line[idx]))
	read_gaz.close()
	return gaz_zcta

def haversine_distance(pt0, pt1):
	lat1, lon1, lat2, lon2 = map(math.radians, [float(pt0[0]), float(pt0[1]), float(pt1[0]), float(pt1[1])])
	# Haversine formula
	dlat = lat2 - lat1
	dlon = lon2 - lon1
	a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
	# Radius of the Earth in kilometers (mean value)
	radius_earth_km = 6371.0
	# Calculate the Haversine distance in kilometers
	distance_km = radius_earth_km * c
	# Convert kilometers to miles (1 kilometer = 0.621371 miles)
	distance_miles = distance_km * 0.621371
	return distance_miles
#####################################################################################
#####################################################################################
conn_fedContracts = sqlite3.connect('fed_contracts.db')  # Replace 'example.db' with your database name
cursor_fedContracts = conn_fedContracts.cursor()
#CSAFP	GEOID	NAME	CBSA_TYPE	ALAND	AWATER	ALAND_SQMI	AWATER_SQMI	INTPTLAT	INTPTLONG  
tableId = str('dot')
query = f"SELECT * FROM {tableId}"
cursor_fedContracts.execute(query)
rows = cursor_fedContracts.fetchall()

'''
0:'contractId', 1:'zipcode',  2:'stateCode', 3:'permalink', 4:'description',
5:'actionDate', 6:'startDate', 7:'endDate', 8:'naicsDesc', 9:'naicsCode', 
10:'recipient', 11:'fedActionObligation', 12:'totDolar', 13:'fundingSubAgency',
14:'zipcode_lat', 15:'zipcode_lon'''
zctaPoints = []
for row in rows:
	zctaPoints.append(Point(row[15], row[14]))

gdf_zctaPoints = gpd.GeoDataFrame(geometry=zctaPoints)
#####################################################################################


naicsRef_ = {}
with open("%s%s" % (r"resources/naics/","naicsCodes.csv"), 'r', newline='') as csvfile:
    csv_reader = csv.reader(csvfile)
    for row in csv_reader:
        naicsRef_[str(row[0])] = str(row[1])

headerRef_json = json.load(open("%s%s" % (r"resources/","headerRef.json")))
#####################################################################################
newFeatureCollection = {'type': 'FeatureCollection', 'features': []}
#####################################################################################
geojson_path = r'resources/tx_counties.geojson'
with fiona.open(geojson_path, 'r') as featureCollection:
	for i, feature in enumerate(featureCollection):
		#feature_properties = feature['properties']
		feature_id = feature['id']
		feature_geometry = feature['geometry']['coordinates']

		newFeature = {
			"type": "Feature",
			'feature_id':feature_id,
			"geometry": {
				"type": "Polygon",
				"coordinates": feature_geometry
			},
			'properties': {
				'country_name':None,
				'cbsa_geoid': None,
				"zip_codes":[],
				'contract_id':[],
				'total_dollars':0,
				'mean_contract_value':0,
				'contract_count':0,
				'contracts_by_naics':{}
			}
		}

		polygon_coordinates = []
		for pt in feature_geometry[0]:
			polygon_coordinates.append([pt[0], pt[1]])

		feature_polygon = Polygon(polygon_coordinates)
		feature_polygon = feature_polygon.buffer(0)
		
		gdf_polygon = gpd.GeoDataFrame(geometry=[feature_polygon])
		points_in_polygon = gpd.sjoin(gdf_zctaPoints, gdf_polygon, how='inner', predicate='within')
		points_index = list(points_in_polygon.index)

		#GEOID	ALAND	AWATER	ALAND_SQMI	AWATER_SQMI	INTPTLAT	INTPTLONG
		total_dollars = []
		contracts_by_naics = {}
		if len(points_index) != 0:
			for idx in list(points_in_polygon.index):
				filteredFeature = rows[idx]
				contractId = filteredFeature[0]
				totDolar = int(float(filteredFeature[12]))
				naicsCode = filteredFeature[9]

				if naicsCode not in list(contracts_by_naics.keys()):
					contracts_by_naics[naicsCode] = {
						'total_dollars':[totDolar],
						}
				else:
					contracts_by_naics[naicsCode]['total_dollars'].append(totDolar)

				newFeature['properties']['contract_id'].append(contractId)
				total_dollars.append(totDolar)

			for naicsCode, contractData in contracts_by_naics.items():
				newFeature['properties']['contracts_by_naics'][naicsCode] = {
					'total_dollars':sum(contractData['total_dollars']),
					'count':len(contractData['total_dollars']),
					'mean_dollars':np.mean(contractData['total_dollars'])
					}

			#feature['properties']['zip_codes'].append(geoid)
		if len(total_dollars) != 0:
			newFeature['properties']['total_dollars'] = sum(total_dollars)
			newFeature['properties']['mean_contract_value'] = np.mean(total_dollars)
			newFeature['properties']['contract_count'] = len(total_dollars)
		
		newFeatureCollection['features'].append(newFeature)

with open(os.path.join(r"output", "tx_counties_fedContracts.geojson"), "w", encoding='utf-8') as output_json:
	json.dump(newFeatureCollection, output_json, indent=1, ensure_ascii=False)
print("DONE")
		
