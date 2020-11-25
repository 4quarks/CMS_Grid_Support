# coding=utf-8

from abc import ABC
from utils.query_utils import AbstractQueries, Time

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

https://monit-kibana-acc.cern.ch/kibana/goto/a297815a5ad0689f8dfa5cb6b4c0a8ef
"""


class SAMTest(AbstractQueries, ABC):
    def __init__(self, time_slot):
        super().__init__(time_slot)
        self.index_name = "monit_prod_sam3_enr_*"
        self.index_id = "9677"

    def get_details_test(self, hostname, metric_name=""):
        kibana_query = "data.vo:cms AND data.dst_hostname:{} " \
                       "AND data.status:CRITICAL " \
                       "AND data.metric_name:/.*{}.*/".format(hostname,metric_name)
        response = self.get_direct_response(kibana_query=kibana_query)

        # all_logs = []
        # for error in response:
        #     description_error = error['data']['details']
        #     all_logs.append(description_error)

        return response


if __name__ == "__main__":
    time = Time(hours=2)
    sam = SAMTest(time)

    print(sam.get_details_test("se3.itep.ru"))

    # r = []
    # for error in response:
    #
    #     description_error = error['data']['details']
    #     # a = re.findall("error accessing (grid[0-9]+.physics.uoi.gr)", str(description_error))
    #     a = re.findall("/store/test/xrootd/T1_IT_CNAF(.*).root", str(description_error))
    #     if a and a[0] not in r:
    #         r.append(a[0])
    #         print(a[0])
    print()


"""
class TestsSRM:
    ipv6 = "org.cms.DNS-IPv6"
    all_cms = "org.cms.SRM-AllCMS-/cms/Role=production"
    pfn_from_tfc = "org.cms.SRM-GetPFNFromTFC-/cms/Role=production"
    vo_del = "org.cms.SRM-VODel-/cms/Role=production"
    get = "org.cms.SRM-VOGet-/cms/Role=production"
    vo_get = "org.cms.SRM-VOGetTURLs-/cms/Role=production"
    vo_ls = "org.cms.SRM-VOLs-/cms/Role=production"
    vo_ls_dir = "org.cms.SRM-VOLsDir-/cms/Role=production"
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


class Status:
    error = "CRITICAL"
    ok = "OK"
    warning = "WARNING"


class SAMAttributes:
    attr_hostname = "data.dst_hostname:"
    attr_status = "data.status:"
    attr_test_name = "data.metric_name:"

def get_specific_query(self, specific_test=None, status=None):
    raw_query = self.attr_hostname + self.hostname
    if specific_test:
        raw_query += Cte.AND + self.attr_test_name + Cte.QUOTE + specific_test + Cte.QUOTE
    if status:
        raw_query += Cte.AND + self.attr_status + status

    clean_str_query = get_str_lucene_query(self.index['name'],
                                           self.time_slot[0], self.time_slot[1], raw_query)

    return clean_str_query

query_critical_jobsubmit = sam.get_specific_query(specific_test=sam.ce.job_submit, status=sam.ok)

"""










































































































