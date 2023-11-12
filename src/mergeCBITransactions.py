import numpy as np
import pandas as pd
import time
import os
from os import listdir
from os.path import isfile, join
import json
from pathlib import Path
#from dotenv import load_dotenv
import os

cbi_downloads_energy = r""

print(cbi_downloads_energy)
files_ = [f for f in listdir(cbi_downloads_energy) if isfile(join(cbi_downloads_energy, f))]
######################################################################################
d_combined = {
	"Business Relationships":{
		"Date":[],"Business Partner":[],"Type":[],
		"Country":[],"News Snippet":[]
		},
	"Investments":{
		"Date":[],"Company":[],"Amount":[],
		"Round":[],"New/Follow-On":[],"Co-Investors":[]
		},
	"Investors":{
		"First Funding":[],"Last Funding":[],"Investor":[],
		"Type":[],"Location":[],"Rounds":[],"Board Seats":[]
		},
	"Fundings":{
		"Round":[],"Date":[],"Amount (M)":[],
		"Investors":[],"Valuation Min (M)":[],"Valuation Max (M)":[],
		"Revenue Min (M)":[],"Revenue Max (M)":[],
		"Revenue Multiple Min (M)":[],"Revenue Multiple Max (M)":[],"Revenue Time Period":[]
		},
	"Acquisitions":{
		"Date":[],"Company":[],"Valuation (min)":[],
		"Valuation (max)":[],"Investment Stage":[],"Note":[]
		},
	"Exits":{
		"Date":[],"Company":[],"Valuation (min)":[],
		"Valuation (max)":[],"Exit":[],"Acquirer":[]
		}
}

transactionsCombined = {}
######################################################################################
def add_data(data_,entityName,category):
	dict_ = transactionsCombined[entityName][category]
	for key in list(dict_.keys()):
		for val in list(data_[key]):
			if val is None:
				val = "NONE"
			dict_[key].append(val)
######################################################################################
categories = list(d_combined.keys())

for f in files_:
	fname = f.split(".")[0]
	fname_split = fname.split(" - ")
	d_category = fname_split[-1]
	if d_category in categories:
		print(f)
		fpath = cbi_downloads_energy+"%s" % (f)
		d_ = pd.read_csv(fpath,encoding="utf-8", low_memory=False)
		entityName = fname_split[0]
		#add_data(d_,entityName,f_category)
		if entityName not in list(transactionsCombined.keys()):
			transactionsCombined[entityName] = d_combined
			add_data(d_,entityName,d_category)
		else:
			add_data(d_,entityName,d_category)
######################################################################################
with open(str("%s%s" % (cbi_downloads_energy,"transactions_combined.json")), "w", encoding='utf-8') as json_output:
	json_output.write(json.dumps(transactionsCombined, indent=2, ensure_ascii=False))
