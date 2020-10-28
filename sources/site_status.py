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
metadata.path 	down15min

data.detail 	All CEs (ce-1.grid.vbc.ac.at), all XROOTDs (eos.grid.vbc.ac.at)
data.duration 	1,603,346,400, 1,603,807,200
data.status 	downtime

---------------------------------------------------------------------------------
fts15min, sam15min, hc15min, sr15min --> "15min", "1hour", "6hour", "1day"
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


metadata.path 	sr1hour
data.detail 	SAM: ok (15min evaluations: 4 ok, 0 warning, 0 error, 0 unknown, 0 downtime (0h0m)),
                HC: ok (7 Success ...; 1 Success, no Chirp ExitCode ...),
                FTS: ok (Links: 9/9 ok, 0/3 warning, 0/0 error, 1/1 unknown, 0/0 bad-endpoint; storage01.lcg.cscs.ch: ok/ok)
data.value 	0.75

	
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


class AbstractSiteStatus(AbstractQueries, ABC):
    def __init__(self, str_freq, time_slot, site_name, metric, specific_fields=None):
        super().__init__(time_slot)
        self.index_name = "monit_prod_cmssst*"
        self.index_id = "9475"
        self.metric = metric
        self.specific_fields = specific_fields

        self.str_freq = str_freq

        self.site_name = site_name

        self.response = self.get_response_shortcut()

    def get_kibana_query(self):
        kibana_query = "metadata.path:" + self.metric + self.str_freq
        if self.site_name:
            kibana_query += "AND data.name:"+self.site_name
        return kibana_query

    def get_response_shortcut(self):
        kibana_query = self.get_kibana_query()
        return self.get_response(self.get_query(kibana_query))

    def get_specific_data(self, test=None):
        specific_data = {}
        if self.specific_fields:
            for field_name in self.specific_fields:
                if field_name and test and field_name in test["data"].keys():
                    field_value = test["data"][field_name]
                    specific_data.update({field_name: field_value})
        return specific_data

    def get_common_data(self, test):
        status = test["data"]["status"]
        timestamp = test["metadata"]["timestamp"]
        timestamp_hr = timestamp_to_human_utc(timestamp)
        detail = test["data"]["detail"]

        return status, timestamp, timestamp_hr, detail

    def get_issues(self):
        issues = []
        for test in self.response:
            issue = {}
            status, timestamp, timestamp_hr, detail = self.get_common_data(test)
            issue_occurred = True
            if status == "ok":
                issue_occurred = False
            if issue_occurred:
                issue.update(self.get_specific_data(test))
                # Details
                if detail:
                    issue.update({"detail": detail})
                issue.update({"status": status, "timestamp": timestamp, "timestamp_hr": timestamp_hr})
                issues.append(issue)
        return issues


class SAM(AbstractSiteStatus):
    def __init__(self, str_freq, time_slot, site_name=""):
        super().__init__(str_freq, time_slot, site_name=site_name, metric="sam")


class HammerCloud(AbstractSiteStatus):
    def __init__(self, str_freq, time_slot, site_name=""):
        super().__init__(str_freq, time_slot, site_name=site_name, metric="hc")


class FTS(AbstractSiteStatus):
    def __init__(self, str_freq, time_slot, site_name=""):
        super().__init__(str_freq, time_slot, site_name=site_name, metric="fts", specific_fields=["quality"])


class SiteReadiness(AbstractSiteStatus):
    def __init__(self, site_name, str_freq, time_slot):
        super().__init__(str_freq, time_slot, site_name=site_name, metric="sr", specific_fields=["value"])


class Downtime(AbstractSiteStatus):
    def __init__(self, str_freq, time_slot, site_name=""):
        super().__init__(str_freq, time_slot, site_name=site_name, metric="down", specific_fields=["duration"])


class SiteStatus(AbstractSiteStatus):
    def __init__(self, str_freq, time_slot, site_name=""):
        specific_fields = ["prod_status", "crab_status", "manual_life", "manual_prod", "manual_crab"]
        super().__init__(str_freq, time_slot, site_name=site_name, metric="sts", specific_fields=specific_fields)


if __name__ == "__main__":
    time = Time(hours=24).time_slot
    sam = SiteStatus("15min", time, site_name="T2_AT_Vienna")
    print(sam.get_issues())

