import sqlite3
import pandas as pd
import time

# Connect to the SQLite database
conn = sqlite3.connect('uscb_gazetter.db')
cursor = conn.cursor()

def get_gaz(labels,fname):
	gaz_zcta = {}
	for l in labels:
		gaz_zcta[l] = []
	with open(str("%s%s" % (r"resources/uscb/gazetter/",fname)),encoding="utf-8") as read_gaz:
		gaz_lines = read_gaz.readlines()
		headers_ = list(map(lambda h: str(h).strip(),gaz_lines[0].split("	")))
		#print(headers_)
		idx_ = []
		for l in labels:
			idx_.append(headers_.index(l))
		for i in range(1,len(gaz_lines),1):
			gaz_line = gaz_lines[i].split("	")
			for l,idx in zip(labels,idx_):
				gaz_zcta[l].append(str(gaz_line[idx]).strip())
	read_gaz.close()
	return gaz_zcta
######################################################################################
gaz_zcta = get_gaz(['GEOID','ALAND','AWATER','ALAND_SQMI','AWATER_SQMI','INTPTLAT','INTPTLONG'],"2022_Gaz_zcta_national.txt")

for geoid, aland, awater, aland_sqmi, awater_sqmi, intplat, intplong in zip(
	gaz_zcta['GEOID'], gaz_zcta['ALAND'], gaz_zcta['AWATER'], gaz_zcta['ALAND_SQMI'],
	gaz_zcta['AWATER_SQMI'], gaz_zcta['INTPTLAT'], gaz_zcta['INTPTLONG']
):

    cursor.execute('''
        INSERT INTO zcta_2021 (geoid, aland, awater, aland_sqmi, awater_sqmi, intplat, intplong)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (geoid, aland, awater, aland_sqmi, awater_sqmi, intplat, intplong))

conn.commit()
time.sleep(1)

######################################################################################
#CSAFP	GEOID	NAME	CBSA_TYPE	ALAND	AWATER	ALAND_SQMI	AWATER_SQMI	INTPTLAT	INTPTLONG
gaz_cbsa = get_gaz([
	'CSAFP','GEOID','NAME','CBSA_TYPE',
	'ALAND','AWATER','ALAND_SQMI', 
	'AWATER_SQMI', 'INTPTLAT', 
	'INTPTLONG'],"2021_Gaz_cbsa_national.txt")

for csafp, geoid, name, cbsa_type, aland, awater, aland_sqmi, awater_sqmi, intplat, intplong in zip(
	gaz_cbsa['CSAFP'], gaz_cbsa['GEOID'], gaz_cbsa['NAME'], gaz_cbsa['CBSA_TYPE'],
	gaz_cbsa['ALAND'], gaz_cbsa['AWATER'], gaz_cbsa['ALAND_SQMI'], gaz_cbsa['AWATER_SQMI'],
	gaz_cbsa['INTPTLAT'], gaz_cbsa['INTPTLONG']
):
	#print(geoid, csafp, name, cbsa_type, aland, awater, aland_sqmi, awater_sqmi, intplat, intplong)

    cursor.execute('''
        INSERT INTO cbsa_2021 (geoid, csafp, name, cbsa_type, aland, awater, aland_sqmi, awater_sqmi, intplat, intplong)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (geoid, csafp, name, cbsa_type, aland, awater, aland_sqmi, awater_sqmi, intplat, intplong))

conn.commit()
conn.close()
print("DATA ADDED TO: ", 'cbsa_2021')
