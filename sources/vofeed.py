import datetime
import json
from query_utils import get_data_grafana, get_str_lucene_query


"""
data.services 	{
                  "hostname": "xrootd01-cmst1.pic.es",
                  "flavour": "XROOTD",
                  "endpoint": "xrootd01-cmst1.pic.es:1095"
                },
                {
                  "hostname": "xrootd02-cmst1.pic.es",
                  "flavour": "XROOTD",
                  "endpoint": "xrootd02-cmst1.pic.es:1095"
                }
data.site 	T1_ES_PIC
data.tier 	1
data.vo 	CMS
metadata.path 	vofeed15min
metadata.producer 	cmssst
"""


query = "metadata.path:vofeed15min AND data.site:T1_ES_PIC"


cmssst_index = {"name": "monit_prod_cmssst*", "id": "9475"}

now_datetime = datetime.datetime.now()
yesterday_datetime = now_datetime - datetime.timedelta(hours=1)
max_time = round(datetime.datetime.timestamp(now_datetime)) * 1000
min_time = round(datetime.datetime.timestamp(yesterday_datetime)) * 1000


clean_str_query = get_str_lucene_query(cmssst_index['name'], min_time, max_time, query)
response = json.loads(get_data_grafana(cmssst_index['id'], clean_str_query).text.encode('utf8'))

print(response)


























