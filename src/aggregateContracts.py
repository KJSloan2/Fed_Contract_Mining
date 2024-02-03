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
import geopy.distance

def format_zipcode(zipcode,targetLen):
	zipcode=str(zipcode)
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
	with open(str("%s%s" % (r"00_resources/gaz/",fname)),encoding="utf-8") as read_gaz:
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
#####################################################################################
#####################################################################################
gaz_zcta = get_gaz(["GEOID","INTPTLAT","INTPTLONG"],"2023_Gaz_zcta_national.txt")
gaz_cbsa = get_gaz(["GEOID","NAME","INTPTLAT","INTPTLONG"],"2023_Gaz_cbsa_national.txt")
#####################################################################################
zipcodeRef_ = json.load(open("%s%s" % (r"00_resources/","zipCodeStateRef.json")))
#naicsRef_ = json.load(open("%s%s" % (r"00_resources/","naicsCodes.json")))
naicsRef_ = {}
with open("%s%s" % (r"00_resources/naics/","naicsCodes.csv"), 'r', newline='') as csvfile:
    csv_reader = csv.reader(csvfile)
    for row in csv_reader:
        naicsRef_[str(row[0])] = str(row[1])

headerRef_json = json.load(open("%s%s" % (r"00_resources/fedContracts/","headerRef.json")))
#####################################################################################
#fName_funds_contract = "FY2023_069_Contracts_Full_20230713_1.csv"
fName_funds_contract = input("Enter the name of the contract dataset to analyize: ")
agencyCode = fName_funds_contract.split("_")[1]
fy = fName_funds_contract.split("_")[0]
#####################################################################################
funds_contract = pd.read_csv(str("%s%s%s" % (r"01_data/fedContracts/",fName_funds_contract,".csv")),encoding="utf-8", low_memory=False)
d_zipcodes = list(funds_contract[headerRef_json["zipcodes"][agencyCode]])
d_stateCodes = list(funds_contract[headerRef_json["stateCodes"][agencyCode]])
d_permalinks = list(funds_contract[headerRef_json["permalinks"][agencyCode]])
d_descriptions = list(funds_contract[headerRef_json["descriptions"][agencyCode]])
d_actionDates  = list(funds_contract[headerRef_json["actionDates"][agencyCode]])
d_startDates  = list(funds_contract[headerRef_json["startDates"][agencyCode]])
d_endDates  = list(funds_contract[headerRef_json["endDates"][agencyCode]])
d_naicsDesc  = list(funds_contract[headerRef_json["naicsDesc"][agencyCode]])
d_naicsCodes  = list(funds_contract[headerRef_json["naicsCodes"][agencyCode]])
d_recipient = list(funds_contract[headerRef_json["recipient"][agencyCode]])
d_fedActionObligations  = list(funds_contract[headerRef_json["fedActionObligations"][agencyCode]])
d_totDolars = list(funds_contract[headerRef_json["totDolars"][agencyCode]])
d_subAgency  = list(funds_contract[headerRef_json["fundingSubAgency"][agencyCode]])
#####################################################################################
geoCrosswalk_ = json.load(open("%s%s" % (r"00_resources/","GAZ_Crosswalk.json")))
cbsaKeys_ = list(geoCrosswalk_.keys())
#####################################################################################
cbsa_stateCodes = []
for i in range(len(gaz_cbsa["GEOID"])):
	stateCode = str(list(gaz_cbsa["NAME"][i].split(", "))[-1]).split(" ")[0]
	cbsa_stateCodes.append(stateCode)

get_locid = list(gaz_cbsa["GEOID"])
get_lat = list(gaz_cbsa["INTPTLAT"])
get_lon = list(gaz_cbsa["INTPTLONG"])
get_stateCode = cbsa_stateCodes
#####################################################################################
zcRef_stateCodes = []
for obj in zipcodeRef_:
	zcRef_stateCodes.append(obj["state_code"])

naicsRef_ncais = list(naicsRef_.keys())

ui_maxDist = input("input the maximum distance (in miles) from the inertpolated center of each CBSA to get contract data: ")
ui_filterNaics = input("filter by NAICS codes?: (y/n)")
#####################################################################################
cbsaStats_ = {}
recipientStats_ = {}
#####################################################################################
with open("%s%s" % (r"02_output/fedContracts/","_byCbsa.csv"),'w',newline='', encoding='utf-8') as write_dataOut:
	writer_dataOut = csv.writer(write_dataOut)
	writer_dataOut.writerow([
		"LOCID","LAT","LON","STATE_CODE","ZIPCODE",
		"DIST","FUND_TYPE","NAICS_CODE","RECIPIENT",
		"ACTION_DATE","START_DATE","END_DATE","PERMALINK",
		"FED_ATION_OBL","CURRENT_TOTAL_VALUE","SUBAGENCY","DESCRIPTION"
		])
	
	for i in range(len(get_locid)):
		query_ = {
			"locid":get_locid[i],
			"coords":[get_lat[i],get_lon[i]],
			"maxdist":ui_maxDist,
			"state_code":get_stateCode[i],
			"geoids":[],
			"dist":[]
		}
	for i in range(len(d_zipcodes)):
		if str(d_zipcodes[i]).strip() != "" and len(str(d_zipcodes[i])) >=5:
			zipcode = zipcode = format_zipcode(d_zipcodes[i],5)
			for cbsaKey in cbsaKeys_:
				cbsaObj_ = geoCrosswalk_[cbsaKey]
				cbsaLat = cbsaObj_["cbsa_lat"]
				cbsaLon = cbsaObj_["cbsa_lon"]
				if zipcode in cbsaObj_["zcta"]["geoid"]:
					idx_zipcode = cbsaObj_["zcta"]["geoid"].index(zipcode)
					dist = round(
						(float(geopy.distance.geodesic((
							cbsaObj_["zcta"]["lat"][idx_zipcode],
	                        cbsaObj_["zcta"]["lon"][idx_zipcode]),
			                (cbsaLat,cbsaLon)).miles)),2)
					if dist <= int(ui_maxDist):
						recipient = d_recipient[i]
						naicsCode = str(d_naicsCodes[i])
						naics_description = None
						if naicsCode in naicsRef_ncais:
							naics_description = naicsRef_[naicsCode]

						if cbsaKey not in list(cbsaStats_.keys()):
							cbsaStats_[cbsaKey] = {
								"geometry":[cbsaLon,cbsaLat],
								"cbsa_name":cbsaObj_["cbsa_name"],
								"awards_total":int(d_totDolars[i])
							}
						else:
							cbsaStats_[cbsaKey]["awards_total"]+=int(d_totDolars[i])

						if recipient not in list(recipientStats_.keys()):
							recipientStats_[recipient] = {
									"naics_code":naicsCode,
									"naics_desc":naics_description,
									"awards_total":int(d_totDolars[i]),
									"locations":{}
								}
						else:
							recipientStats_[recipient]["awards_total"] += int(d_totDolars[i])

						writer_dataOut.writerow([
							cbsaObj_["cbsa_name"],cbsaLat,cbsaLon,
							query_["state_code"],zipcode,dist,"contract",naics_description,d_recipient[i],
							d_actionDates[i],d_startDates[i],d_endDates[i],
							d_permalinks[i],
							d_fedActionObligations[i],
							d_totDolars[i],
							d_subAgency[i],
							d_naicsDesc[i]
							])
					
write_dataOut.close()
with open(str("%s%s%s%s%s%s" % (r"02_output/fedContracts/",agencyCode,"_",fy,"_","cbsaStats.json")), "w") as json_cbsaStats:
	json_cbsaStats.write(json.dumps(cbsaStats_, indent=2, ensure_ascii=False))

with open(str("%s%s%s%s%s%s" % (r"02_output/fedContracts/",agencyCode,"_",fy,"_","recipientStats.json")), "w") as json_recipientStats:
	json_recipientStats.write(json.dumps(recipientStats_, indent=2, ensure_ascii=False))
	
	
print("done")
