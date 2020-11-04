from query_utils import *
from vofeed import VOFeed
from sam3 import SAMTest
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
NUM_ERRORS_ROW = 8
HOURS_RANGE = 5
NUM_TESTS_PER_HOUR = 4
TOTAL_TESTS_EVALUATED = HOURS_RANGE * NUM_TESTS_PER_HOUR
PERCENT_MIN_OK = 0.7  # 20 --> 14

NUM_MIN_OK = int(HOURS_RANGE * NUM_TESTS_PER_HOUR * PERCENT_MIN_OK)


class AbstractSiteStatus(AbstractQueries, ABC):
    def __init__(self, time_class, site_name="", str_freq="15min", specific_fields=None, flavour=None):
        super().__init__(time_class)
        self.index_name = "monit_prod_cmssst*"
        self.index_id = "9475"

        self.specific_fields = specific_fields

        self.str_freq = str_freq

        self.site_name = site_name

        self.vofeed = VOFeed(0)
        self.flavour = flavour
        self.site_resources = self.vofeed.get_resource_filtered(site=site_name, flavour=flavour)

        self.sam3 = SAMTest(time_class)

        print()

    def get_kibana_query(self, metric, status="", name="", flavour=""):
        kibana_query = "metadata.path:" + metric + self.str_freq
        if name:
            kibana_query += Cte.AND + "data.name:" + name
        if status:
            kibana_query += Cte.AND + "data.status:" + status
        if flavour:
            kibana_query += Cte.AND + "data.type:" + flavour
        return kibana_query

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

    def get_issues(self, response_all):
        status = "ok"
        list_errors = []

        response_not_ok = [test for test in response_all if test["data"]["status"] != "ok"]

        num_not_ok_tests = int(len(response_not_ok))
        tests_evaluated = len(response_all)
        num_ok_tests = tests_evaluated - num_not_ok_tests
        percent = num_ok_tests / tests_evaluated

        # IN A ROW EVALUATION
        if num_ok_tests < NUM_MIN_OK:
            status_in_a_row = ""
            num_errors_row = tests_evaluated
            for num_error, test in enumerate(reversed(response_all)):
                status = test["data"]["status"]
                if not status_in_a_row:
                    status_in_a_row = status
                if status != status_in_a_row:
                    num_errors_row = num_error
                    break
            print("ERROR: {} oks, {} not oks --> out of {} --> {} oks".format(num_ok_tests, num_not_ok_tests,
                                                                              tests_evaluated, percent))
            print("{} {} in a row".format(num_errors_row, status_in_a_row))

            # DETAILED ERROR EVALUATION
            for test in response_not_ok:
                issue = {}
                # status, timestamp, timestamp_hr, details = self.get_common_data(test)
                # issue.update(self.get_specific_data(test))
                # Details
                if "detail" in test["data"].keys():
                    details = test["data"]["detail"]
                    metrics = details.split(",")
                    for elem in metrics:
                        if elem:
                            metric_error = elem.split("(")[0].strip()
                            if metric_error not in list_errors and test["data"]["status"] == "error":
                                list_errors.append(elem.split("(")[0].strip())
                    print()
                    # issue.update({"detail": split_details})
                # issue.update({"status": status, "timestamp": timestamp, "timestamp_hr": timestamp_hr})
                # info_error.append(issue)
        else:
            print("OK")
        return status, list_errors

    def remove_duplicate_responses(self, response_all):
        times = []
        clean_response = []
        for response in response_all:
            time_test = response["metadata"]["timestamp"]
            if time_test not in times:
                times.append(time_test)
                clean_response.append(response)
        return clean_response

    def get_status(self):
        metrics = ["sam", "hc", "fts"]
        print(self.site_name, "\n")
        for metric in metrics:
            kibana_query_all = self.get_kibana_query(metric, name=self.site_name)
            response_all = self.get_direct_response(kibana_query=kibana_query_all)
            clean_response = self.remove_duplicate_responses(response_all)
            print(metric.upper())
            issues_sam1 = self.get_issues(clean_response)

    def get_issues_resources(self):
        for resource in self.site_resources:
            hostname = resource["hostname"]
            flavour = resource["flavour"]
            print("{} --> {}".format(flavour, hostname))
            if "production" not in resource.keys():
                if "CE" in flavour:
                    flavour = "CE"
                if "XROOTD" in flavour:
                    flavour = "XRD"
                kibana_query_all = self.get_kibana_query("sam", name=hostname, flavour=flavour)
                response_all = self.get_direct_response(kibana_query=kibana_query_all)
                status, list_errors = self.get_issues(response_all)
                if status != "ok" and list_errors:
                    errors_dict = {}
                    if status == "error":
                        for metric_with_error in list_errors:
                            logs = self.sam3.get_details_test(hostname=hostname, metric_name=metric_with_error)
                            times_saved = []
                            clean_logs = []
                            for log in logs:
                                timestamp = log["data"]["timestamp"]
                                if timestamp not in times_saved:
                                    times_saved.append(timestamp)
                                    clean_logs.append(log)
                            errors_dict.update({metric_with_error: clean_logs})
                print()
            else:
                print("Test endpoint")

            print("*" * 20)

    # def get_resources_not_running_tests(self):
    #     flavours = ["CE", "SRM", "XRD"]
    #     sites = {}
    #     for flavour in flavours:
    #         kibana_query_all = self.get_kibana_query(flavour=flavour, status="unknown")
    #         response_all = self.get_direct_response(kibana_query=kibana_query_all, max_results=2000)
    #         for element in response_all:
    #             data_site = self.vofeed.get_resource_filtered(flavour=flavour, hostname=element["data"]["name"])
    #             site_name = data_site[0]["site"]
            #     sites.update({site_name:})
            # print(site)


# class SAM(AbstractSiteStatus):
#     def __init__(self, str_freq, time_slot, site_name=""):
#         super().__init__(str_freq, time_slot, site_name=site_name, metric="sam")
#
#
# class HammerCloud(AbstractSiteStatus):
#     def __init__(self, str_freq, time_slot, site_name=""):
#         super().__init__(str_freq, time_slot, site_name=site_name, metric="hc")
#
#
# class FTS(AbstractSiteStatus):
#     def __init__(self, str_freq, time_slot, site_name=""):
#         super().__init__(str_freq, time_slot, site_name=site_name, metric="fts", specific_fields=["quality"])
#
#
# class SiteReadiness(AbstractSiteStatus):
#     def __init__(self, site_name, str_freq, time_slot):
#         super().__init__(str_freq, time_slot, site_name=site_name, metric="sr", specific_fields=["value"])
#
#
# class Downtime(AbstractSiteStatus):
#     def __init__(self, str_freq, time_slot, site_name=""):
#         super().__init__(str_freq, time_slot, site_name=site_name, metric="down", specific_fields=["duration"])
#
#
# class SiteStatus(AbstractSiteStatus):
#     def __init__(self, str_freq, time_slot, site_name=""):
#         specific_fields = ["prod_status", "crab_status", "manual_life", "manual_prod", "manual_crab"]
#         super().__init__(str_freq, time_slot, site_name=site_name, metric="sts", specific_fields=specific_fields)


if __name__ == "__main__":
    time = Time(hours=HOURS_RANGE)
    sam = AbstractSiteStatus(time, site_name="T2_RU_ITEP")
    # sam.get_status()
    sam.get_issues_resources()

