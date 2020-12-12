# coding=utf-8

from cms_support.utils.constants import CteRucio
import time
from copy import deepcopy
from abc import ABC
from cms_support.utils.query_utils import AbstractQueries
from cms_support.utils.nlp_utils import group_data
import re


class Transfers(AbstractQueries, ABC):
    def __init__(self, time_class, target=""):
        super().__init__(time_class, target)
        self.index_name = CteRucio.INDEX_ES
        self.index_id = CteRucio.INDEX_ES_ID
        self.basic_kibana_query = "data.vo:cms AND data.event_type:{}".format("transfer-failed")

    @staticmethod
    def split_pfn_info(raw_pfn):
        lfn, protocol, se = "", "", ""
        if raw_pfn:
            se = raw_pfn.split("/")[2]
            protocol = raw_pfn.split("/")[0].strip(":").strip()
            if "/store" in raw_pfn:
                lfn = "/store" + raw_pfn.split("/store")[1]
        return lfn, protocol, se

    def analyze_site(self, filter_error_kibana="", blacklist_regex="",):
        """
        Get dict with all the errors grouped by: host, destination/origin, type of error
        """
        all_data, data_host = {}, {}
        site, hostname = self.target, ""
        # IF THE TARGET IS NOT A SITE
        if self.target and not re.search("T[0-9]_*", self.target):
            hostname, site = CteRucio.ADD_STR.format(self.target), ""

        ############ GET DATA ORIGIN AND DESTINATION ############
        directions = ["src", "dst"]
        for direction in directions:
            other_direction = "dst"
            if direction == "dst":
                other_direction = "src"
            time.sleep(0.1)
            dict_attr = {direction + "_url": hostname,
                         direction + "_rse": site,
                         CteRucio.REF_LOG: filter_error_kibana}
            dict_attr_not = {other_direction + "_url": blacklist_regex,
                             other_direction + "_rse": blacklist_regex,
                             other_direction + "_endpoint": blacklist_regex}
            kibana_query = self.get_kibana_query(dict_attr=dict_attr, dict_attr_not=dict_attr_not)
            print('epa ', kibana_query)
            ############ QUERY TO ELASTICSEARCH ############
            response_kibana = self.get_direct_response(kibana_query=kibana_query, max_results=10000)
            ############ GROUP DATA BY ERRORS ############
            grouped_by_error = {}
            ############ ITERATE OVER ALL ERRORS ############
            for error in response_kibana:
                error_data = deepcopy(error[CteRucio.REF_DATA])
                src_url, dst_url = error_data[CteRucio.REF_PFN_SRC], error_data[CteRucio.REF_PFN_DST]
                ############ AVOID ERRORS THAT ARE BLACKLISTED ############ç
                # in_blacklist = self.is_blacklisted(src_url, dst_url, blacklist_regex)
                # Extract useful data
                if CteRucio.REF_LOG in error_data.keys():  # and not in_blacklist:
                    ############ ADD EXTRA DATA ############
                    src_lfn, src_protocol, src_se = self.split_pfn_info(src_url)
                    dst_lfn, dst_protocol, dst_se = self.split_pfn_info(dst_url)

                    error_data.update({CteRucio.REF_LFN_SRC: src_lfn, CteRucio.REF_LFN_DST: dst_lfn,
                                       CteRucio.REF_SE_SRC: src_se, CteRucio.REF_SE_DST: dst_se,
                                       "src_protocol": src_protocol, "dst_protocol": dst_protocol,
                                       'name': error_data['name']})

                    ############ GROUP THE ERROR ############
                    attributes_if_match_same_error = [CteRucio.REF_PFN_SRC, CteRucio.REF_PFN_DST]
                    grouped_by_error = group_data(grouped_by_error, error_data, attributes_if_match_same_error,
                                                  CteRucio)
            if grouped_by_error:
                data_host.update({direction: grouped_by_error})

        all_data.update({self.target: data_host})
        return all_data


# if __name__ == "__main__":
#     from cms_support.utils.query_utils import Time
#     from cms_support.utils.transfers_utils import ExcelGenerator
#     time_class = Time(days=1, hours=0, minutes=0)
#     fts = Transfers(time_class)
#     dict_result = fts.analyze_site(target="T2_HU_Budapest", str_blacklist="se3.itep.ru/se01.indiacms.res.in")
#     generator = ExcelGenerator(dict_result, "T2_HU_Budapest")
#     generator.results_to_csv(write_lfns=False)
