from constants import CteRucio
import time
from copy import deepcopy
from abc import ABC
from query_utils import AbstractQueries, Time
import argparse
from transfers_utils import ExcelGenerator, TextNLP

"""
######################## monit_prod_cms_rucio_enr*  ########################
query = "data.name:T2_US_Florida AND data.status:ok AND metadata.path:hc15min"
rucio_index = {"name": "monit_prod_cms_rucio_enr*", "id": "9732"}

data.dst_rse 	T2_KR_KISTI
data.src_rse 	T2_US_Wisconsin

data.activity 	Production Output|User Subscriptions
data.bytes 	111,809,924
data.file_size 	111,809,924
data.checksum_adler 	c88b8895
data.file_id 	2867026540
data.name 	/store/mc/RunIIAutumn18NanoAODv7/VBFHHTo2G2ZTo2G.../x.root
data.src_url 	srm://gfe02.grid.hep.ph.ic.ac.uk:8443/srm/managerv2?SFN=/pnfs/hep.ph.ic.ac.uk/data/cms/.../tree_54.root
data.dst_url 	srm://stormfe1.pi.infn.it:8444/srm/managerv2?SFN=/cms/.../tree_54.root

-------------------- SUBMITTED -------------------------------
data.event_type     transfer-submitted
data.state 	SUBMITTED

data.external_host 	https://fts3.cern.ch:8446
data.queued_at 	2020-10-06 12:58:45.839821
data.request_type 	transfer
data.scope 	cms
data.vo 	cms
metadata.producer 	cms_rucio
-------------------- FAILED -------------------------------
data.event_type     transfer-failed
data.state	unknown
data.purged_reason 	DESTINATION [17] Destination file exists and overwrite is not enabled
data.reason 	DESTINATION [17] Destination file exists and overwrite is not enabled

-------------------- OK -------------------------------
data.event_type     transfer-done

https://monit-kibana.cern.ch/kibana/goto/31129642d019649a5fe8ee3c816677e6
"""


class Transfers(AbstractQueries, ABC):
    def __init__(self, time_class):
        super().__init__(time_class)
        self.index_name = CteRucio.INDEX_ES
        self.index_id = CteRucio.INDEX_ES_ID

    @staticmethod
    def is_blacklisted(src_url, dest_url):
        """
        Check if the pfn contains blacklisted elements
        :param src_url: 'gsiftp://eoscmsftp.cern.ch//eos/cms/store/temp/user/cc/.../out_offline_Photons_230.root'
        :param dest_url:
        :return:
        """
        black_pfn = [black_pfn for black_pfn in BLACKLIST_PFN if black_pfn in src_url or black_pfn in dest_url]
        return black_pfn

    def build_query_get_response(self, direction, site="", hostname="", filter_error_kibana=""):
        """

        :param hostname: name of the host to analyze e.g. eoscmsftp.cern.ch
        :param direction: s / d
        :param filter_error_kibana: keyword of the error to find e.g. "No such file"
        :return:
        """
        ############ CONSTRUCT THE QUERY ############
        kibana_query_failed = "data.vo:cms AND data.event_type:{}".format("transfer-failed")
        if hostname:
            kibana_query_failed += CteRucio.ADD_DATA.format(direction + "_url", hostname)
        if site:
            kibana_query_failed += CteRucio.ADD_DATA.format(direction + "_rse", site)
        if filter_error_kibana:
            kibana_query_failed += CteRucio.ADD_DATA.format(CteRucio.REF_LOG, filter_error_kibana)

        ############ QUERY TO ELASTICSEARCH ############
        response_failed = self.get_direct_response(kibana_query=kibana_query_failed, max_results=10000)
        return response_failed

    @staticmethod
    def split_pfn_info(raw_pfn):
        lfn, protocol, se = "", "", ""
        if raw_pfn:
            se = raw_pfn.split("/")[2]
            protocol = raw_pfn.split("/")[0].strip(":").strip()
            if "/store" in raw_pfn:
                lfn = "/store" + raw_pfn.split("/store")[1]
        return lfn, protocol, se

    def analyze_site(self, target="", filter_error_kibana=""):
        """
        Get dict with all the errors grouped by: host, destination/origin, type of error
        """
        all_data, data_host = {}, {}
        site, hostname = target, ""
        if target and "T" not in target[0]:
            hostname, site = target, ""

        ############ GET DATA ORIGIN AND DESTINATION ############
        for direction in ["src", "dst"]:
            time.sleep(0.1)
            response_kibana = self.build_query_get_response(direction, site=site, hostname=hostname,
                                                            filter_error_kibana=filter_error_kibana)
            ############ GROUP DATA BY ERRORS ############
            grouped_by_error = {}
            ############ ITERATE OVER ALL ERRORS ############
            for error in response_kibana:
                error_data = deepcopy(error[CteRucio.REF_DATA])
                src_url, dst_url = error_data[CteRucio.REF_PFN_SRC], error_data[CteRucio.REF_PFN_DST]
                ############ AVOID ERRORS THAT ARE BLACKLISTED ############ç
                in_blacklist = self.is_blacklisted(src_url, dst_url)
                # Extract useful data
                if CteRucio.REF_LOG in error_data.keys() and not in_blacklist:
                    ############ ADD EXTRA DATA ############
                    src_lfn, src_protocol, src_se = self.split_pfn_info(src_url)
                    dst_lfn, dst_protocol, dst_se = self.split_pfn_info(dst_url)

                    error_data.update({CteRucio.REF_LFN_SRC: src_lfn, CteRucio.REF_LFN_DST: dst_lfn,
                                       CteRucio.REF_SE_SRC: src_se, CteRucio.REF_SE_DST: dst_se,
                                       "src_protocol": src_protocol, "dst_protocol": dst_protocol,
                                       'name': error_data['name']})

                    ############ GROUP THE ERROR ############
                    attributes_if_match_same_error = [CteRucio.REF_PFN_SRC, CteRucio.REF_PFN_DST]
                    grouped_by_error = group_data(grouped_by_error, error_data, attributes_if_match_same_error)
            if grouped_by_error:
                data_host.update({direction: grouped_by_error})
        all_data.update({hostname: data_host})
        return all_data


def group_data(grouped_by_error, error_data, list_same_ref):
    ############ ADD EXTRA INFO ############
    error_log = error_data[CteRucio.REF_LOG]
    error_data.update({CteRucio.REF_NUM_ERRORS: 1})

    ########### DETECT KEYWORDS ###########
    error_nlp = TextNLP(error_log, CteRucio.KEYWORDS_ERRORS)
    previous_errors = list(grouped_by_error.keys())
    keyword = error_nlp.get_keyword(error_nlp.clean_text)
    if keyword:
        error_ref = keyword
    else:
        error_ref = error_log
    ############ CHOOSE REFERENCE ############
    if keyword not in previous_errors and error_log not in previous_errors:
        # If the error was not grouped --> add
        grouped_by_error.update({error_ref: [error_data]})
    else:
        # If the error match with another --> count repetition
        exactly_same_error = False
        # EXACT SAME ERROR REPEATED --> num_errors + 1
        for idx, element in enumerate(grouped_by_error[error_ref]):
            # SAME ERROR & SAME PFN --> SAME ISSUE
            for reference in list_same_ref:
                if element[reference] == error_data[reference]:
                    exactly_same_error = True
                else:
                    exactly_same_error = False
            if exactly_same_error:
                grouped_by_error[error_ref][idx][CteRucio.REF_NUM_ERRORS] += 1
                break
        if not exactly_same_error:
            grouped_by_error[error_ref].append(error_data)
    return grouped_by_error


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("hours", help="Number of hours to analyze", type=int)
    parser.add_argument("target", help="Site or hostname to analyze", type=str)
    parser.add_argument("--black", help="Blacklisted elements separated by '/'", type=str)
    parser.add_argument("--write_lfns", help="Write files with all LFNS", action="store_true")
    parser.add_argument("--error", help="Keywords to search on the error log (separate by dots '.')", type=str)
    args = parser.parse_args()
    if args.error:
        args.error = args.error.replace(".", " ")
    if args.write_lfns:
        args.write_lfns = True

    time_class = Time(hours=args.hours)
    fts = Transfers(time_class)
    BLACKLIST_PFN = args.black.split("/")
    dict_result = fts.analyze_site(target=args.target, filter_error_kibana=args.error)
    generator = ExcelGenerator(dict_result)
    generator.results_to_csv(write_lfns=args.write_lfns)

    # time_class = Time(hours=12)
    # fts = Transfers(time_class)
    # BLACKLIST_PFN = ["se3.itep.ru", "se01.indiacms.res.in", "cmsio.rc.ufl.edu"]
    # dict_result = fts.analyze_site(site_or_host="T2_ES_IFCA")
    # fts.results_to_csv(dict_result, write_lfns=False)
