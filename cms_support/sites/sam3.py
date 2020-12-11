# coding=utf-8

from cms_support.utils.constants import CteSAM as CteSAM
from cms_support.utils.query_utils import timestamp_to_human_utc, AbstractQueries
from cms_support.utils.nlp_utils import group_data
from cms_support.utils.site_utils import get_resource_from_target
from copy import deepcopy
from abc import ABC


class SAMTest(AbstractQueries, ABC):
    def __init__(self, time_slot):
        super().__init__(time_slot)
        self.index_name = "monit_prod_sam3_enr_*"
        self.index_id = "9677"

    def get_details_test(self, hostname, metric_name=""):
        kibana_query = "data.vo:cms AND data.dst_hostname:{} " \
                       " " \
                       "AND data.metric_name:/.*{}.*/".format(hostname, metric_name)
        response = self.get_direct_response(kibana_query=kibana_query, acc_index=True)
        return response


class SAMSiteStatus(AbstractQueries, ABC):
    def __init__(self, time_class, target="", blacklist_regex=""):
        super().__init__(time_class, target, blacklist_regex)
        self.index_name = "monit_prod_sam3_enr_*"
        self.index_id = "9677"
        self.basic_kibana_query = "data.vo:cms AND NOT data.status:OK"

    def get_issues_resources(self):
        ############ GROUP DATA BY ERRORS ############
        grouped_by_site = {}
        site_regex, hostname_regex, flavour_regex = get_resource_from_target(self.target)
        dict_attr = {"dst_experiment_site": site_regex,
                     "dst_hostname": hostname_regex,
                     "service_flavour": flavour_regex}
        kibana_query = self.get_kibana_query(dict_attr=dict_attr)
        response_kibana_sam3 = self.get_direct_response(kibana_query=kibana_query, acc_index=True)
        ############ ITERATE OVER ALL ERRORS ############
        for error in response_kibana_sam3:
            error_data = error["data"]
            status = error_data["status"]
            if status != "CRITICAL":
                error_data["details"] = ""
            site = error_data['dst_experiment_site']
            site_is_blacklisted = self.is_blacklisted(site, self.blacklist_regex)
            test_is_blacklisted = self.is_blacklisted(error_data['metric_name'], CteSAM.NON_INTERESTING_TESTS)
            if not site_is_blacklisted and not test_is_blacklisted:
                if site not in grouped_by_site.keys():
                    grouped_by_site.update({site: {}})
                # Extract useful data
                if CteSAM.REF_LOG in error[CteSAM.REF_DATA].keys():
                    ############ GROUP THE ERROR ############
                    error_data = deepcopy(error[CteSAM.REF_DATA])
                    timestamp_hr = timestamp_to_human_utc(error_data[CteSAM.REF_TIMESTAMP])
                    error_data.update({CteSAM.REF_TIMESTAMP_HR: timestamp_hr})
                    group_data(grouped_by_site[site], error_data, ['metric_name'], CteSAM)
        return grouped_by_site

    @staticmethod
    def get_rows_from_grouped_data(grouped_by_site):
        rows = []
        for site, errors in grouped_by_site.items():
            for error_log, error_data in errors.items():
                for same_group in error_data:
                    timestamp_hr = timestamp_to_human_utc(same_group[CteSAM.REF_TIMESTAMP])
                    row = [timestamp_hr, site, same_group["dst_hostname"], same_group["service_flavour"],
                           same_group["metric_name"], same_group["status"], error_log, same_group[CteSAM.REF_LOG],
                           same_group[CteSAM.REF_NUM_ERRORS]]
                    rows.append(row)
        return rows


# if __name__ == "__main__":
#     from cms_support.utils.site_utils import write_excel
#     from cms_support.utils.query_utils import Time
#     time = Time(hours=4)
#     sam3 = SAMSiteStatus(time, target="T1|T2", blacklist_regex="itep")
#     grouped_data = sam3.get_issues_resources()
#     rows = sam3.get_rows_from_grouped_data(grouped_data)
#     write_excel(rows, columns=columns, name_ref="testing")


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
