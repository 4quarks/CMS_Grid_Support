import datetime
import json
from query_utils import get_data_grafana, get_str_lucene_query
from constants import Constants as Cte

"""
---------------------------------------------------------------------------------
sts15min
----------
data.name 	T1_DE_KIT

metadata.path 	sts15min 

data.status 	enabled
data.prod_status 	drain
data.crab_status 	enabled

data.manual_life 	enabled
data.manual_prod 	disabled
data.manual_crab 	enabled

data.detail 	Life: manual override by rmaciula (Tier-1s are never put into Waiting Room or Morgue state),
                Prod: Emerging downtime, [current],
                Crab: manual override by lammel (very few CRAB jobs currently being scheduled at KIT)
---------------------------------------------------------------------------------
fts15min, sam15min, hc15min
----------
data.detail 	3 Success [crab3@vocms0137.cern.ch#55922252.0#1601902070 201005_001640:sciaba_crab_ .....
data.name 	T2_IT_Bari
data.status 	ok
metadata.path 	hc15min
-------------
data.detail 	org.cms.SE-xrootd-read (error)
data.name 	grid71.phy.ncu.edu.tw
data.status 	error
data.type 	XRD
metadata.path 	sam15min
-------------
data.detail 	trn_timeout: 34 files, 0.0 GB [https://fts3.cern.ch:8449/fts3/ftsmon/#/job/3b664b36-070c-11eb...]
                trn_error: 32 files, 0.0 GB [https://fts3.cern.ch:8449/fts3/ftsmon/#/job/d518140c-070d-11eb-a...]
                dst_space: 3 files, 0.0 GB [https://fts3.cern.ch:8449/fts3/ftsmon/#/job/66147c40-070c-11eb-91...]
data.name 	cmsio.rc.ufl.edu___eoscmsftp.cern.ch
data.quality 	0
data.status 	error
data.type 	link
metadata.path 	fts15min
---------------------------------------------------------------------------------

"""


# query_hc = "data.name:T2_US_Florida AND data.status:ok AND metadata.path:hc15min"
#
# query_sam_test = "data.dst_hostname:lcgce02.phy.bris.ac.uk AND data.metric_name:'org.sam.CONDOR-JobSubmit-/cms/Role=lcgadmin'"
#
# hostname = "lcgce02.phy.bris.ac.uk"
# status = "CRITICAL"
# query_sam_failed = "data.dst_hostname:{} AND data.status:{}".format(hostname, status)
#
#
#
#
# now_datetime = datetime.datetime.now()
# yesterday_datetime = now_datetime - datetime.timedelta(hours=1)
# max_time = round(datetime.datetime.timestamp(now_datetime)) * 1000
# min_time = round(datetime.datetime.timestamp(yesterday_datetime)) * 1000
#
#





