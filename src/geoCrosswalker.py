import numpy as np
import json
import geopy.distance
import datetime
from datetime import datetime, timezone
from os import listdir
from os.path import isfile, join
import os
#####################################################################################
'''geoCatchBasin: the type of geographic tabulation area to serve as the base 
location to pool surrounding tabulation areas with'''
geoCatchBasin = "cbsa"
##############################GLOBAL FUNCTIONS#######################################
def check_encoding(file2check):
	file_encoding = ""
	with open(file2check) as file_info:
		file_encoding = file_info.encoding
	file_info.close()
	print(file_encoding)
	return str(file_encoding)

def get_gaz(tab,labels):
	'''retreives geographic data from Gazzetter files using the arguments to specify the 
	file to read and columns to extract.
	'tab': the tabulation type (cnty, cbsa, ztca, etc.), used to grab the file name from
	 the 'gazFileNames_' dictionary.
	'labels': a list of column labels to get from the file ('GEOID', 'NAME', 'INTPTLAT', etc.)'''
	gaz_ = {}
	for l in labels:
		gaz_[l] = []
	with open(str("%s%s" % (dirpaths_["gaz"],gazFileNames_[tab])),encoding="utf-8") as read_gaz:
		gaz_lines = read_gaz.readlines()
		headers_ = list(map(lambda h: str(h).strip(),gaz_lines[0].split("	")))
		print(headers_)
		idx_ = []
		for l in labels:
			idx_.append(headers_.index(l))
		for i in range(1,len(gaz_lines),1):
			gaz_line = gaz_lines[i].split("	")
			for l,idx in zip(labels,idx_):
				gaz_[l].append(str(gaz_line[idx]))
	read_gaz.close()
	return gaz_

def crosswalk(gaz_,gcw_,tab,maxDist):
	''' 'gaz_': the Gazzetter data extracted by the 'get_gaz' function
		'gcw_': the geocrosswalk
		'tab': the tabulation type for geographic elements to pool with the geoCatchBasin
		'maxDist': the upper threshold for distances considered valid. If a distance is greater,
		location is ignored.
		The crosswalk function cycles over each geoCatchBasic object in the geoCrosswalk_
		and each geographic element in the given gaz_ and measures the distance (in miles)
		from the geographic element to each geoCatchBasin. The index of the minimum distance is 
		taken and used to get the key of the nearest geoCatchBasin. The geographic element is then
		added to the geoCatchBasin object.'''
	dictKeys_ = list(gcw_.keys())
	for geoid,lat,lon in zip(gaz_["GEOID"],gaz_["INTPTLAT"],gaz_["INTPTLONG"]):
		tempDists_ = []
		tempKeys_ = []
		for key in dictKeys_:
			dist = round((float(geopy.distance.geodesic((lat,lon),(gcw_[key]["INTPTLAT"],gcw_[key]["INTPTLONG"])).miles)),2)
			if dist <= maxDist:
				tempKeys_.append(key)
				tempDists_.append(dist)
		if len(tempDists_) != 0:
			#key_nearest = tempKeys_[np.argsort(tempDists_)[0]]
			distMin = min(tempDists_)
			key_nearest = tempKeys_[tempDists_.index(distMin)]
			print(tab," :",geoid," CBSA: ",key_nearest," Dist:",distMin)
			if tab not in list(gcw_[key_nearest].keys()):
				gcw_[key_nearest][tab] = {"GEOID":[geoid],"INTPTLAT":[lat],"INTPTLONG":[lon],"DIST":[distMin]}
			else:
				gcw_[key_nearest][tab]["GEOID"].append(geoid)
				gcw_[key_nearest][tab]["INTPTLAT"].append(float(lat))
				gcw_[key_nearest][tab]["INTPTLONG"].append(float(lon))
				gcw_[key_nearest][tab]["DIST"].append(distMin)
				

'''def get_nearby(gaz_,parselen):
	temp_ = {}
	keys_ = []
	for geoid,lat,lon in zip(gaz_["GEOID"],gaz_["INTPTLAT"],gaz_["INTPTLONG"]):
		geoId_st = geoid[0:parselen]
		if geoId_st not in list(temp_.keys()):
			keys_.append(geoId_st)
			temp_[geoId_st] = {"lat":[float(lat)],"lon":[float(lon)]}
		else:
			temp_[geoId_st]["lat"].append(float(lat))
			temp_[geoId_st]["lon"].append(float(lon))

	for geoId_st in keys_:
		lat_ = temp_[geoId_st]["lat"]
		lon_ = temp_[geoId_st]["lon"]
		geoExtents = {
			"center": [np.mean(lat_),np.mean(lon_)],
			"sw": [min(lat_)-lat_offset,min(lon_)-lon_offset],
			"se": [min(lat_)-lat_offset,max(lon_)+lon_offset],
			"nw": [max(lat_)+lat_offset,min(lon_)-lon_offset],
			"ne": [max(lat_)+lat_offset,max(lon_)+lon_offset]
		}
		print(
			geoExtents["center"][0],
			geoExtents["center"][1],
			geoExtents["sw"][0],
			geoExtents["sw"][1],
			geoExtents["se"][0],
			geoExtents["se"][1],
			geoExtents["nw"][0],
			geoExtents["nw"][1],
			geoExtents["ne"][0],
			geoExtents["ne"][1]
			)'''

'''def get_nearby(gaz_,ref):
	for geoid,lat,lon in zip(gaz_["GEOID"],gaz_["INTPTLAT"],gaz_["INTPTLONG"]):
		for stKey in list(ref["zipcodes_state_ref"].keys()):
			for zc in ref["zipcodes_state_ref"][stKey]:
				if str(geoid[0:len(zc)]) == zc:
					print(geoid,stKey)'''

lat_offset = 0.0144927536231884*50
lon_offset = 0.0183150183150183*50
def bounding_box(gaz_):
	temp_ = {}
	for geoid,lat,lon in zip(gaz_["GEOID"],gaz_["INTPTLAT"],gaz_["INTPTLONG"]):
		geoid_chunk = str(geoid[0:2])
		if geoid_chunk not in list(temp_.keys()):
			temp_[geoid_chunk] = {
				"GEOID":[geoid],
				"INTPTLAT":[float(lat)],
				"INTPTLONG":[float(lon)],
				"bounding_box":{"center":None,"sw":None,"se": None,"nw":None,"ne":None},
				"geoids_nearby":[]
			}
		else:
			temp_[geoid_chunk]["GEOID"].append(geoid)
			temp_[geoid_chunk]["INTPTLAT"].append(float(lat))
			temp_[geoid_chunk]["INTPTLONG"].append(float(lon))
	for key in (list(temp_.keys())):
		temp_[key]["bounding_box"]["center"] = [
			np.mean(temp_[key]["INTPTLAT"]),np.mean(temp_[key]["INTPTLONG"])
			]
		temp_[key]["bounding_box"]["sw"] = [
			min(temp_[key]["INTPTLAT"]),min(temp_[key]["INTPTLONG"])
			]
		temp_[key]["bounding_box"]["se"] = [
			min(temp_[key]["INTPTLAT"]),max(temp_[key]["INTPTLONG"])
			]				
		temp_[key]["bounding_box"]["nw"] = [
			max(temp_[key]["INTPTLAT"]),min(temp_[key]["INTPTLONG"])
			]			
		temp_[key]["bounding_box"]["ne"] = [
			max(temp_[key]["INTPTLAT"]),max(temp_[key]["INTPTLONG"])
			]
	return temp_

#####################################################################################
##############################DIRECTORY PATHS########################################
#####################################################################################
'''Names of the Gazzetter files. The 'tab' argument plugged into the 'get_gaz' function,
is used to retreive the correct Gazzetter file name from this dict'''
gazFileNames_ = {
	"elsd":"2021_Gaz_elsd_national.txt",
	"place":"2021_Gaz_place_national.txt",
	"cnty": "2021_Gaz_counties_national.txt",
	"zcta": "2021_Gaz_zcta_national.txt",
	"cd115":"2017_Gaz_115CDs_national.txt",
	"cbsa": "2021_Gaz_cbsa_national.txt",
	}
#####################################################################################
#####################################################################################

refCbsa_= []
with open(str("%s%s" % (dirpaths_["gaz"],"USCB_SBP_CBSA.txt")),encoding="utf-8") as read_refCbsa:
	refCbsa_lines = read_refCbsa.readlines()
	for i in range(len(refCbsa_lines)):
		refCbsa_.append(refCbsa_lines[i].strip())
read_refCbsa.close()

args_ = {
	"cbsa":{"process":True,"parselen":2,"labels":["GEOID","NAME","INTPTLAT","INTPTLONG"],"maxDist": 100},
	"cd115":{"process":True,"parselen":2,"labels":["GEOID","INTPTLAT","INTPTLONG"],"maxDist": 100},
	"cnty":{"process":True,"parselen":2,"labels":["GEOID","NAME","INTPTLAT","INTPTLONG"],"maxDist": 100},
	"zcta":{"process":True,"parselen":2,"labels":["GEOID","INTPTLAT","INTPTLONG"],"maxDist": 100},
	"place":{"process":True,"parselen":5,"labels":["GEOID","NAME","INTPTLAT","INTPTLONG"],"maxDist": 100}
}
#zipcodeRef_ = json.load(open("%s%s" % (dirpaths_["00_resources"],"zipcode_state_ref.json")))
#get_nearby(get_gaz("zcta",args_["zcta"]["labels"]),zipcodeRef_)

zcta_pooled = bounding_box(get_gaz("zcta",args_["zcta"]["labels"]))
cbsa_ = get_gaz("cbsa",args_["cbsa"]["labels"])

def get_nearby(pooled_,gcb_):
	for bbKey in list(pooled_.keys()):
		bb = pooled_[bbKey]["bounding_box"]
		temp_dists = []
		for ptKey in ["center","sw","se","nw","ne"]:
			temp_dists.append(round((float(geopy.distance.geodesic((bb["center"][0],bb["center"][1]),(bb[ptKey][0],bb[ptKey][1])).miles)),2))
		maxDist = np.mean(temp_dists)+np.std(temp_dists)
		for geoid,lat,lon in zip(gcb_["GEOID"],gcb_["INTPTLAT"],gcb_["INTPTLONG"]):
			dist = round((float(geopy.distance.geodesic(
				(bb["center"][0],bb["center"][1]),
				(float(lat),float(lon))
				).miles)),2)
			if dist <= maxDist:
				if geoid not in pooled_[bbKey]["geoids_nearby"]:
					pooled_[bbKey]["geoids_nearby"].append(geoid)
	return(pooled_)

print(get_nearby(zcta_pooled,cbsa_))
