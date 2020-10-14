import datetime
import json
from query_utils import get_data_grafana, get_str_lucene_query, get_clean_results
from constants import Constants as Cte

"""
data.details  OK: COMPLETED.<br>>>> ce05-htc.cr.cnaf.infn.it: ....
data.dst_country Italy
data.dst_experiment_site 	T1_IT_CNAF
data.dst_federation IT-INFN-CNAF
data.dst_hostname ce05-htc.cr.cnaf.infn.it
data.dst_site INFN-T1
data.dst_tier 1
data.gathered_at tf-cms-prod.cern.ch
data.metric_name org.sam.CONDOR-JobSubmit-/cms/Role=lcgadmin
data.nagiosHost UNKNOWN
data.service_flavour HTCONDOR-CE
data.status OK
data.status_code 1
data.summary OK: COMPLETED.
data.vo cms

"""


class Status:
    error = "CRITICAL"
    ok = "OK"
    warning = "WARNING"


class TestsSRM:
    ipv6 = "org.cms.DNS-IPv6"
    # "org.cms.SRM-AllCMS-/cms/Role=production"
    # "org.cms.SRM-GetPFNFromTFC-/cms/Role=production"
    # "org.cms.SRM-VODel-/cms/Role=production"
    get = "org.cms.SRM-VOGet-/cms/Role=production"
    # "org.cms.SRM-VOGetTURLs-/cms/Role=production"
    # "org.cms.SRM-VOLs-/cms/Role=production"
    # "org.cms.SRM-VOLsDir-/cms/Role=production"
    put = "org.cms.SRM-VOPut-/cms/Role=production"


class TestsCE:
    analysis = "org.cms.WN-analysis-/cms/Role=lcgadmin"
    basic = "org.cms.WN-basic-/cms/Role=lcgadmin"
    cvmfs = "org.cms.WN-cvmfs-/cms/Role=lcgadmin"
    env = "org.cms.WN-env-/cms/Role=lcgadmin"
    frontier = "org.cms.WN-frontier-/cms/Role=lcgadmin"
    isolation = "org.cms.WN-isolation-/cms/Role=lcgadmin"
    mc = "org.cms.WN-mc-/cms/Role=lcgadmin"
    squid = "org.cms.WN-squid-/cms/Role=lcgadmin"
    access = "org.cms.WN-xrootd-access-/cms/Role=lcgadmin"
    fallback = "org.cms.WN-xrootd-fallback-/cms/Role=lcgadmin"
    job_submit = "org.sam.CONDOR-JobSubmit-/cms/Role=lcgadmin"


class TestsXROOTD:
    connection = "org.cms.SE-xrootd-connection"
    contain = "org.cms.SE-xrootd-contain"
    read = "org.cms.SE-xrootd-read"
    version = "org.cms.SE-xrootd-version"


class TestsName:
    def __init__(self):
        self.srm = TestsSRM()
        self.ce = TestsCE()
        self.xrootd = TestsXROOTD()


class SAMAttributes:
    attr_hostname = "data.dst_hostname:"
    attr_status = "data.status:"
    attr_test_name = "data.metric_name:"


class SAMTest(TestsName, SAMAttributes, Status):
    def __init__(self, hostname, specific_test=None, status=None, time='12'):
        super().__init__()
        self.cmssst_index = {"name": "monit_prod_sam3_enr_*", "id": "9677"}
        self.hostname = hostname
        self.raw_time = time

        self.time_slot = self.translate_time()

    def translate_time(self):
        now_datetime = datetime.datetime.now()
        previous_datetime = now_datetime - datetime.timedelta(hours=int(self.raw_time))
        max_time = round(datetime.datetime.timestamp(now_datetime)) * 1000
        min_time = round(datetime.datetime.timestamp(previous_datetime)) * 1000
        return [min_time, max_time]

    def get_query_tests(self, specific_test=None, status=None):
        query_sam = self.attr_hostname + self.hostname
        if specific_test:
            query_sam += Cte.AND + self.attr_test_name + Cte.QUOTE + specific_test + Cte.QUOTE
        if status:
            query_sam += Cte.AND + self.attr_status + status

        clean_str_query = get_str_lucene_query(self.cmssst_index['name'],
                                               self.time_slot[0], self.time_slot[1], query_sam)

        return clean_str_query

    def get_response(self, clean_query):
        raw_response = json.loads(get_data_grafana(self.cmssst_index['id'], clean_query).text.encode('utf8'))
        return get_clean_results(raw_response)


if __name__ == "__main__":
    sam = SAMTest('ce05-htc.cr.cnaf.infn.it')
    query_critical_jobsubmit = sam.get_query_tests(specific_test=sam.ce.job_submit, status=sam.ok)
    response = sam.get_response(query_critical_jobsubmit)

    print(response)


















































































































