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

files2merge_ = [
	"DOE_FY2023_089_Contracts_Full_20230208.csv",
	"DOT_FY2023_069_Contracts_Full_20230208.csv",
	"ED_FY2023_091_Contracts_Full_20230208.csv",
	"HHS_FY2023_075_Contracts_Full_20230208.csv",
	"HUD_FY2023_086_Contracts_Full_20230208.csv",
	"NASA_FY2023_080_Contracts_Full_20230208.csv"
]

headers2get_ = [
    "entity","naics_code","funding_office_name","recipient_zip_code","primary_place_of_performance_zip_4","recipient_parent_name",
    "recipient_city_name","recipient_county_name","recipient_state_code",
    "total_dollars_obligated","period_of_performance_start_date",
    "period_of_performance_current_end_date","transaction_description"
]

dataOut_dirPath = r""
dataOut_fileName = "temp_merge.csv"
#write_dataOut = open("%s%s" % (dataOut_dirPath,dataOut_fileName), 'w',newline='', encoding='utf-8')
#writer_dataOut = csv.writer(write_dataOut)

dirpath_data = r""
with open("%s%s" % (dataOut_dirPath,dataOut_fileName), 'w',newline='', encoding='utf-8') as write_dataOut:
	writer_dataOut = csv.writer(write_dataOut)
	headers2write_ = headers2get_
	writer_dataOut.writerow(headers2write_)
	for i in range(len(files2merge_)):
		fName = files2merge_[i]
		entity = fName.split("_")[0]
		print(fName)
		df_ = pd.read_csv(str("%s%s" % (dirpath_data,fName)),encoding="utf-8", low_memory=False)
		headers_ = list(df_.columns)
		rows = df_.shape[0]
		for r in range(rows):
			d_ = [entity]
			for j in range(1,len(headers2get_),1):
				h = headers2get_[j]
				if h in headers_:
					val =  list(df_[h])[r]
					d_.append(val)
				else:
					d_.append("null") 
			writer_dataOut.writerow(d_)

#df_composite = pd.DataFrame(data_composite, columns=headers2get_)
#df_composite.to_csv("%s%s" % (dataOut_dirPath,dataOut_fileName), encoding='utf-8', index=False)
write_dataOut.close()
print("done")
