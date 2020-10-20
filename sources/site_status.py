from query_utils import *


"""
---------------------------------------------------------------------------------
sts15min
----------
data.name 	T1_DE_KIT

metadata.path 	sts15min 

data.status 	enabled   --> life status
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

data.name 	T2_IT_Rome
data.detail 	cmsrm-cream01.roma1.infn.it/CE (unknown)
                cmsrm-cream02.roma1.infn.it/CE (unknown)
                cmsrm-se01.roma1.infn.it/SRM (unknown)
                cmsrm-xrootd01.roma1.infn.it/XRD (unknown)
                cmsrm-xrootd02.roma1.infn.it/XRD (unknown)

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
HC find job with error:
https://monit-kibana.cern.ch/kibana/goto/d9b1f68e937ac932bd1cd5941541b4a5
https://monit-kibana.cern.ch/kibana/goto/beceac1344613eeeba9d95edba4c97be

data.GlobalJobId:"crab3@vocms0197.cern.ch#57013833.0#1602556487"
data.CRAB_Workflow:"201012_164326:sciaba_crab_HC-98-T2_AT_Vienna-93179-20201012183702"
data.ExitCode:139
data.RemoveReason:/"Removed due to wall clock limit"/
"GlobalPool periodic cleanup" --> data.ExitCode:143

"""


class CMSSST(AbstractQueries, ABC):
    def __init__(self, time_slot):
        super().__init__(time_slot)
        self.index_name = "monit_prod_cmssst*"
        self.index_id = "9475"


class SiteStatus(CMSSST):
    def __init__(self, site_name, time_slot):
        super().__init__(time_slot)
        self.site_name = site_name
        self.query = "data.name:{} AND metadata.path:{}"

    def get_response_shortcut(self, test):
        return self.get_response(self.get_query(self.query.format(self.site_name, test)))

    def get_evaluation_hc(self):
        response = self.get_response_shortcut("hc15min")
        for hc_test in response:
            status = hc_test["data"]["status"]
            if status == "error":
                detail_error = hc_test["data"]["detail"]
                print()

        print()

    def get_evaluation_sam(self):
        response = self.get_response(self.get_query(self.get_kibana_query("sam15min")))


    def get_evaluation_fts(self):
        response = self.get_response(self.get_query(self.get_kibana_query("fts15min")))


    def get_override(self):
        response = self.get_response(self.get_query(self.get_kibana_query("sts15min")))



if __name__ == "__main__":
    time = Time(days=6).time_slot
    sam = SiteStatus("T2_AT_Vienna", time)
    sam.get_evaluation_hc()
    # kibana_query = "data.name:T1_UK_RAL AND metadata.path:hc15min"
    #
    # query_general = sam.get_query(kibana_query=kibana_query)
    #
    # response = sam.get_response(query_general)

    # print(response)





