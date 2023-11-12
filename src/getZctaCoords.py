import numpy as np
from datetime import datetime, timezone
from os import listdir
from os.path import isfile, join
import os
import pandas as pd
import csv
#####################################################################################
def check_encoding(file2check):
	file_encoding = ""
	with open(file2check) as file_info:
		file_encoding = file_info.encoding
	file_info.close()
	print(file_encoding)
	return str(file_encoding)

def get_gaz(tab,labels,fnames_):
	'''retreives geographic data from Gazzetter files using the arguments to specify the 
	file to read and columns to extract.
	'tab': the tabulation type (cnty, cbsa, ztca, etc.), used to grab the file name from
	 the 'gazFileNames_' dictionary.
	'labels': a list of column labels to get from the file ('GEOID', 'NAME', 'INTPTLAT', etc.)'''
	
	gaz_ = {}
	for l in labels:
		gaz_[l] = []
	with open(str("%s%s" % (paths_["gaz"],fnames_[tab])),encoding="utf-8") as read_gaz:
		gaz_lines = read_gaz.readlines()
		headers_ = list(map(lambda h: str(h).strip(),gaz_lines[0].split("	")))
		print(headers_)
		idx_ = []
		for l in labels:
			idx_.append(headers_.index(l))
		for i in range(1,len(gaz_lines),1):
			gaz_line = gaz_lines[i].split("	")
			for l,idx in zip(labels,idx_):
				gaz_[l].append(str(gaz_line[idx].strip()))
	read_gaz.close()
	return gaz_

def format_zipcode(zipcode):
	zipcode=str(zipcode)
	zipcodeFormated = None
	if len(zipcode) == 9:
		z = zipcode[0:-4]
		if len(z) <5:
			chars_ = []
			for i in range(abs(len(z)-5)):
				chars_.append(str(0))
			for c in z:
				chars_.append(c)
			zipcodeFormated = "".join(chars_)
		elif len(z) == 5:
			zipcodeFormated=str(z)
	elif len(zipcode) <5:
		chars_ = []
		for i in range(abs(len(zipcode)-5)):
			chars_.append(str(0))
		for c in zipcode:
			chars_.append(c)
		zipcodeFormated = "".join(chars_)
	elif len(zipcode) == 5:
		zipcodeFormated = zipcode
	return zipcodeFormated

##############################DIRECTORY PATHS########################################
paths_ = {
	"gaz":r"\\DFW-01-DATA01\Design$\Hugo\00_Data Repo\00_By Government Entity\USCB\Geographic Reference\GAZETTEER\\",
	}

gazFileNames_ = {
	"elsd":"2021_Gaz_elsd_national.txt",
	"place":"2021_Gaz_place_national.txt",
	"cnty": "2021_Gaz_counties_national.txt",
	"zcta": "2021_Gaz_zcta_national.txt",
	"cd115":"2017_Gaz_115CDs_national.txt",
	"cbsa": "2021_Gaz_cbsa_national.txt",
	}

####################################################################################

gaz_ = get_gaz("zcta",["GEOID","INTPTLAT","INTPTLONG"],gazFileNames_)

geoids_to_get = []
with open(str("%s%s" % (r"00_resources\\","zipcodes.txt")),encoding="utf-8") as read_geoids:
	lines_ = read_geoids.readlines()
	for i in range(0,len(lines_),1):
		geoid = lines_[i].strip()
		geoids_to_get.append(format_zipcode(geoid))

with open("%s%s" % (r"00_resources\\","selectZctaCoords.txt"), 'w') as write_dataOut:
	for i in range(len(geoids_to_get)):
		geoid = str(geoids_to_get[i])
		if geoid in gaz_["GEOID"]:
			idx = gaz_["GEOID"].index(geoid)
			write_dataOut.write(
				"%s%s%s%s%s%s" % (geoid,",",gaz_["INTPTLAT"][idx],",",gaz_["INTPTLONG"][idx],"\n")
				)
write_dataOut.close()