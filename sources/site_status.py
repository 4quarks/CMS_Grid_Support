import datetime
import json
from query_utils import get_data_grafana, get_str_lucene_query

"""
fts15min, sam15min, hc15min



data.detail 	3 Success [crab3@vocms0137.cern.ch#55922252.0#1601902070 201005_001640:sciaba_crab_ .....
data.name 	T2_IT_Bari
data.status 	ok
metadata.path 	hc15min



data.detail 	org.cms.SE-xrootd-read (error)
data.name 	grid71.phy.ncu.edu.tw
data.status 	error
data.type 	XRD
metadata.path 	sam15min



data.detail 	trn_timeout: 34 files, 0.0 GB [https://fts3.cern.ch:8449/fts3/ftsmon/#/job/3b664b36-070c-11eb...]
                trn_error: 32 files, 0.0 GB [https://fts3.cern.ch:8449/fts3/ftsmon/#/job/d518140c-070d-11eb-a...]
                dst_space: 3 files, 0.0 GB [https://fts3.cern.ch:8449/fts3/ftsmon/#/job/66147c40-070c-11eb-91...]
data.name 	cmsio.rc.ufl.edu___eoscmsftp.cern.ch
data.quality 	0
data.status 	error
data.type 	link
metadata.path 	fts15min
"""

query = "data.name:T2_US_Florida AND data.status:ok AND metadata.path:hc15min"

cmssst_index = {"name": "monit_prod_cmssst*", "id": "9475"}

now_datetime = datetime.datetime.now()
yesterday_datetime = now_datetime - datetime.timedelta(hours=1)
max_time = round(datetime.datetime.timestamp(now_datetime)) * 1000
min_time = round(datetime.datetime.timestamp(yesterday_datetime)) * 1000


clean_str_query = get_str_lucene_query(cmssst_index['name'], min_time, max_time, query)
response = json.loads(get_data_grafana(cmssst_index['id'], clean_str_query).text.encode('utf8'))

print(response)
