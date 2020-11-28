# coding=utf-8

from tools.sites.vofeed import get_resources_from_json
from tools.sites.sam3 import SAMTest
from tools.utils.constants import CteSAM as CteSAM
from abc import ABC
from tools.utils.query_utils import AbstractQueries, Time, timestamp_to_human_utc
from tools.utils.nlp_utils import group_data
from copy import deepcopy
from tools.utils.site_utils import get_resource_from_target, write_excel
import re

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
                FTS: ok (Links: 9/9 ok, 0/3 warning, 0/0 error, 1/1 unknown, 0/0 bad-endpoint; storage01.cscs.ch: ok/ok)
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


class AbstractCMSSST(AbstractQueries, ABC):
    def __init__(self, time_class, metric, str_freq="15min", blacklist_regex=""):
        super().__init__(time_class)
        self.index_name = CteSAM.INDEX_ES
        self.index_id = CteSAM.INDEX_ES_ID
        self.metric = metric
        self.str_freq = str_freq
        self.blacklist_regex = blacklist_regex

    @staticmethod
    def get_ref_flavour(flavour):
        ref_flavour = flavour
        if "CE" in flavour:
            ref_flavour = "CE"
        if "XROOTD" in flavour:
            ref_flavour = "XRD"
        return ref_flavour

    def is_blacklisted(self, target):
        is_blacklisted = False
        if self.blacklist_regex and re.search(self.blacklist_regex.lower(), target.lower()):
            is_blacklisted = True
        return is_blacklisted

    def get_kibana_query(self, metric, name="", dict_features=None):
        kibana_query = "metadata.path:" + metric + self.str_freq
        if name:
            kibana_query += CteSAM.ADD_DATA.format("name", name)

        if dict_features:
            for key, value in dict_features.items():
                if key:
                    kibana_query += CteSAM.ADD_DATA.format(key, value)

        return kibana_query


class SiteStatus(AbstractCMSSST):
    def __init__(self, time_class, target="", flavour="", blacklist_regex=""):
        super().__init__(time_class, CteSAM.REF_SAM_METRIC, blacklist_regex=blacklist_regex)

        self.flavour = self.get_ref_flavour(flavour)

        self.site_name, self.hostname = get_resource_from_target(target)

        self.site_resources = get_resources_from_json(site=self.site_name, hostname=self.hostname, flavour=self.flavour)

        self.sam3 = SAMTest(time_class)

    @staticmethod
    def get_issues(response_all):
        list_errors, all_status = [], []
        num_status_row, num_errors = 0, 0
        for num_test, test in enumerate(response_all):
            test_data = test[CteSAM.REF_DATA]
            status_test = test_data[CteSAM.REF_STATUS]
            all_status.append(status_test)
            if status_test != "ok":
                num_errors += 1
                # DETAILED ERROR EVALUATION
                if CteSAM.REF_LOG_CMSSST in test_data.keys():
                    details = test_data[CteSAM.REF_LOG_CMSSST]
                    if details:
                        metrics = [elem for elem in details.split(",") if elem]
                        for elem in metrics:
                            metric_error = elem.split("(")[0].strip()
                            if metric_error not in list_errors and status_test != "ok":
                                list_errors.append(elem.split("(")[0].strip())
            if all_status and all_status[num_test-1] == status_test:
                num_status_row += 1
        most_frequent_status = max(set(all_status), key=all_status.count)
        return most_frequent_status, list_errors, num_status_row, num_errors

    def get_issues_resources(self):
        rows = []

        for resource in self.site_resources:
            hostname = resource[CteSAM.REF_HOST]
            flavour = resource[CteSAM.REF_FLAVOUR]
            site = resource[CteSAM.REF_SITE]

            is_test_endpoint = "production" in resource.keys()
            is_blacklisted = self.is_blacklisted(site)

            if not is_test_endpoint and not is_blacklisted:
                features_search = {"type": flavour}
                kibana_query_all = self.get_kibana_query(self.metric, name=hostname, dict_features=features_search)
                response_kibana_cmssst = self.get_direct_response(kibana_query=kibana_query_all)
                if response_kibana_cmssst:
                    status, list_errors, num_errors_row, num_not_ok_tests = self.get_issues(response_kibana_cmssst)
                    if status != "ok" and list_errors:
                        if status != "unknown":
                            for metric_with_error in list_errors:
                                response_kibana_sam3 = self.sam3.get_details_test(hostname=hostname,
                                                                                  metric_name=metric_with_error)
                                ############ GROUP DATA BY ERRORS ############
                                grouped_by_error = {}
                                ############ ITERATE OVER ALL ERRORS ############
                                for error in response_kibana_sam3:
                                    # Extract useful data
                                    if CteSAM.REF_LOG in error[CteSAM.REF_DATA].keys():
                                        ############ GROUP THE ERROR ############
                                        error_data = deepcopy(error[CteSAM.REF_DATA])
                                        timestamp_hr = timestamp_to_human_utc(error_data[CteSAM.REF_TIMESTAMP])
                                        error_data.update({CteSAM.REF_TIMESTAMP_HR: timestamp_hr})

                                        grouped_by_error = group_data(grouped_by_error, error_data, ['metric_name'],
                                                                      CteSAM)
                                for error_grouped, value_error in grouped_by_error.items():
                                    for single_error in value_error:
                                        rows.append(
                                            [single_error[CteSAM.REF_TIMESTAMP_HR], site, hostname, flavour, status,
                                             num_not_ok_tests, num_errors_row, metric_with_error, error_grouped,
                                             single_error[CteSAM.REF_NUM_ERRORS]])
                        else:
                            rows.append(
                                ["time", site, hostname, flavour, status, num_not_ok_tests,
                                 num_errors_row,
                                 str(list_errors), None, None])
        return rows


class SiteReadiness(AbstractCMSSST):
    def __init__(self, time_class, site_regex="", blacklist_regex=""):
        super().__init__(time_class, CteSAM.REF_SR_METRIC, blacklist_regex=blacklist_regex)

        if not site_regex:
            site_regex = "*"
        self.site_regex = site_regex

    @staticmethod
    def site_enabled(dict_site, metric):
        site_enabled = True
        if metric in dict_site.keys():
            status = dict_site[metric]
            if status != "enabled":
                site_enabled = False
        return site_enabled

    def get_not_enabled_sites(self, metric=""):
        rows, metrics = [], CteSAM.REF_METRICS_SR

        if metric:
            desired_metrics = [sr_m for m in metric.split("|") for sr_m in CteSAM.REF_METRICS_SR if re.search(m, sr_m)]
            if desired_metrics:
                metrics = desired_metrics
        # for site in self.sites_list:

        kibana_query_all = self.get_kibana_query(self.metric, name=self.site_regex)
        response_kibana_cmssst = self.get_direct_response(kibana_query=kibana_query_all)

        studied_sites = []
        for response in reversed(response_kibana_cmssst):
            data_response = response["data"]
            data_response["life_status"] = data_response.pop("status")  # change key to keep structure <metric>_status
            site_name = data_response["name"]
            is_blacklisted = self.is_blacklisted(site_name)

            if site_name not in studied_sites and not is_blacklisted:
                studied_sites.append(site_name)
                site_with_issues = False

                for metric in metrics:
                    if not self.site_enabled(data_response, metric):
                        site_with_issues = True
                        break
                if site_with_issues:
                    rows.append(data_response)
        return rows

# class Tests:
#     SAM = TestsAbstract(metric="sam")
#     HammerCloud = TestsAbstract(metric="hc")
#     FTS = TestsAbstract(metric="fts", specific_fields=["quality"])
#     SiteReadiness = TestsAbstract(metric="sr", specific_fields=["value"])
#     Downtime = TestsAbstract(metric="down", specific_fields=["duration"])
#     SiteStatus = TestsAbstract(metric="sts", specific_fields=["prod_status", "crab_status", "manual_life",
#                                                               "manual_prod", "manual_crab"])


if __name__ == "__main__":

    time_ss = Time(hours=CteSAM.HOURS_RANGE)
    sam = SiteReadiness(time_ss, site_regex="T2")
    rows = sam.get_not_enabled_sites(metric="prod|life")
    columns = ["name"] + CteSAM.REF_METRICS_SR + ["detail"]
    write_excel(rows, columns=columns, name_ref="testing" + "SiteReadiness")
    # sam.get_status(metrics=[Tests.SAM.metric, Tests.HammerCloud.metric])
    # errors = sam.get_issues_resources()
    print()
