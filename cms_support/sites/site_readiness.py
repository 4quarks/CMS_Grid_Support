# coding=utf-8

from cms_support.utils.constants import CteSAM as CteSAM
from cms_support.utils.site_utils import AbstractCMSSST
import re


class SiteReadiness(AbstractCMSSST):
    def __init__(self, time_class, target="", blacklist_regex=""):
        super().__init__(time_class, CteSAM.REF_SR_METRIC, target=target, blacklist_regex=blacklist_regex)

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
        dict_attr = {"name": self.target}
        kibana_query_all = self.get_kibana_query(dict_attr=dict_attr)
        response_kibana_cmssst = self.get_direct_response(kibana_query=kibana_query_all)

        studied_sites = []
        for response in reversed(response_kibana_cmssst):
            data_response = response["data"]
            data_response["life_status"] = data_response.pop("status")  # change key to keep structure <metric>_status
            site_name = data_response["name"]
            is_blacklisted = self.is_blacklisted(site_name, self.blacklist_regex)

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


if __name__ == "__main__":
    from cms_support.utils.query_utils import Time
    time_ss = Time(hours=CteSAM.HOURS_RANGE)
    sam = SiteReadiness(time_ss, target="T2")
    rows = sam.get_not_enabled_sites(metric="prod|life")
    columns = ["name"] + CteSAM.REF_METRICS_SR + ["detail"]

#     from cms_support.utils.site_utils import write_excel
#     write_excel(rows, columns=columns, name_ref="testing" + "SiteReadiness")
#     print()





















