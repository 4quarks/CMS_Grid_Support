import datetime
import json
from query_utils import get_data_grafana, get_str_lucene_query

"""
data.core_cpu_intensive 3,023
data.core_io_intensive 	45
data.core_max_used 	2,600
data.core_production 	457
data.core_usable 	2,520
data.disk_experiment_use 	1,900
data.disk_pledge 	0
data.disk_usable 	0
data.hs06_per_core 	10
data.hs06_pledge 	0
data.name 	T2_BR_SPRACE
data.tape_pledge 	0
data.tape_usable 	0
data.when 	2020-Jan-17 22:12:00
data.who 	lammel
"""
query = "_exists_:data.core_usable"

cmssst_index = {"name": "monit_prod_cmssst*", "id": "9475"}

now_datetime = datetime.datetime.now()
yesterday_datetime = now_datetime - datetime.timedelta(hours=24)
max_time = round(datetime.datetime.timestamp(now_datetime)) * 1000
min_time = round(datetime.datetime.timestamp(yesterday_datetime)) * 1000


clean_str_query = get_str_lucene_query(cmssst_index['name'], min_time, max_time, query)
response = json.loads(get_data_grafana(cmssst_index['id'], clean_str_query).text.encode('utf8'))

print(response)
