import json
import numpy as np
import pandas as pd
import plotly.express as px

def calc_threshold(d):
	pool_d_pos = []
	pool_d_neg = []
	for i in range(len(d)):
		for j in range(len(d[i])):
			val = d[i][j]
			if val > 0:
				pool_d_pos.append(abs(val))
			elif val < 0:
				pool_d_neg.append(abs(val))
	threshold_pos = np.mean(pool_d_pos)+np.std(pool_d_pos)*2
	threshold_neg = np.mean(pool_d_neg)+np.std(pool_d_neg)*2
	return threshold_pos,threshold_neg*-1;

def threshold_filter(d,thresh_pos,thresh_neg):
	filtered_pos = []
	filtered_neg = []
	for i in range(len(d)):
		for j in range(len(d[i])):
			val = d[i][j]
			if val >= thresh_pos:
					filtered_pos.append([i,j])
			'''elif val <= thresh_neg:
					filtered_neg.append([i,j])'''
	return filtered_pos,filtered_neg;

######################################################################################

#df_ = pd.read_csv(str("%s%s" % (r"02_output/","dot_contracts_byCbsa.csv")),encoding="utf-8")
cbsaStats_json = json.load(open("%s%s" % (r"02_output/","cbsaStats.json")))

data = {
	"CBSA_NAME":[],
	"CBSA_LAT":[],
	"CBSA_LON":[],
	"AWARDS_TOTAL":[]
	}

for cbsaKey,cbsaStats in cbsaStats_json.items():
	data["CBSA_NAME"].append(cbsaStats["cbsa_name"])
	data["CBSA_LON"].append(cbsaStats["geometry"][0])
	data["CBSA_LAT"].append(cbsaStats["geometry"][1])
	data["AWARDS_TOTAL"].append(cbsaStats["awards_total"])

df = pd.DataFrame(data)
fig = px.scatter_mapbox(
	df,
	lat="CBSA_LAT",
	lon="CBSA_LON",
	#hover_name="CD",
	hover_data=["CBSA_NAME","AWARDS_TOTAL"],
	color="AWARDS_TOTAL",
	color_continuous_scale='Inferno',
	#color_continuous_scale=color_scale,
	size="AWARDS_TOTAL",
	zoom=5,
	height=1000,
	width=2500)

fig.update_layout(mapbox_style="carto-darkmatter")
fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
fig.show()

######################################################################################