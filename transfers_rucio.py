import logging
from constants import CteRucio
import time
from copy import deepcopy
from abc import ABC
import re
import pandas as pd
from query_utils import AbstractQueries, Time
import xlsxwriter

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

-------------------- SUBMITTED -------------------------------
data.event_type     transfer-submitted
data.state 	SUBMITTED

data.external_host 	https://fts3.cern.ch:8446
data.name 	/store/mc/RunIIAutumn18NanoAODv7/VBFHHTo2G2ZTo2G.../x.root
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
data.state	unknown

data.transfer_endpoint 	https://fts3.cern.ch:8446
data.src_url 	srm://gfe02.grid.hep.ph.ic.ac.uk:8443/srm/managerv2?SFN=/pnfs/hep.ph.ic.ac.uk/data/cms/.../tree_54.root
data.dst_url 	srm://stormfe1.pi.infn.it:8444/srm/managerv2?SFN=/cms/.../tree_54.root
data.source_se 	srm://dcache-se-cms.desy.de
data.dest_se 	gsiftp://gridftp.accre.vanderbilt.edu
data.name 	/store/.../tree_54.root
"""
"""
##################################### monit_prod_fts_raw_* ###########################

query = "data.vo:cms AND data.dest_se:/.*se.cis.gov.pl.*/ AND data.job_state:FAILED"
fts_index = {"name": "monit_prod_fts_raw_*", "id": "9233"}

query = "data.dst_url:/.*storm-fe-cms.cr.cnaf.infn.it.*/ AND data.file_state:FAILED AND data.reason:/.*CHECKSUM MISMATCH.*/" 

class TransferAttributes:
    attr_destination = "data.dst_url:"
    attr_source = "data.src_url:"
    attr_status = "data.file_state:"
    attr_reason = "data.reason:"


class Status:
    error = "FAILED"
    active = "ACTIVE"
    ready = "READY"
    submitted = "SUBMITTED"
    finished = "FINISHED"

def join_attribute_and_expression(self, attr, expression):
    return CteRucio.AND + attr + CteRucio.EXPRESSION.format(CteRucio.INCLUDED.format(expression))

def get_query(self, dst_url="", dst_protocol="", src_url="", src_protocol="", status="", reason=""):
    raw_query = "data.vo:cms"
    if dst_url:
        raw_query += self.join_attribute_and_expression(self.attr_destination, dst_url)
    if status:
        raw_query += CteRucio.AND + self.attr_status + status

    clean_str_query = get_str_lucene_query(self.index['name'], self.time_slot[0], self.time_slot[1], raw_query)
    return clean_str_query

https://monit-kibana.cern.ch/kibana/goto/31129642d019649a5fe8ee3c816677e6
"""


class Transfers(AbstractQueries, ABC):
    def __init__(self, time_class):
        super().__init__(time_class)
        self.index_name = CteRucio.INDEX_ES
        self.index_id = CteRucio.INDEX_ES_ID

    def is_blacklisted(self, src_url, dest_url):
        """
        Check if the pfn contains blacklisted elements
        :param src_url: 'gsiftp://eoscmsftp.cern.ch//eos/cms/store/temp/user/cc/.../out_offline_Photons_230.root'
        :param dest_url:
        :return:
        """
        black_pfn = [black_pfn for black_pfn in BLACKLIST_PFN if black_pfn in src_url or black_pfn in dest_url]
        return black_pfn

    def get_user(self, url_pfn):
        user = ""
        raw_users = re.findall("/user/(.*)/", str(url_pfn))
        if raw_users:
            user = raw_users[0].split("/")[0].strip("")
            user = user.split(".")[0]  # e.g. gbakas.9c1d054d2d278c14ddc228476ff7559c10393d8d
        if len(raw_users) > 2:
            raise Exception("MULTIPLE USERS ON PFN")
        return user

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

    def analyze_site(self, site_or_host = "", filter_error_kibana=""):
        """
        Get json with all the errors grouped by: host, destination/origin, type of error
        """
        all_data, data_host = {}, {}
        site, hostname = "", ""
        if site_or_host:
            if "T" in site_or_host[0]:
                site = site_or_host
            else:
                hostname = site_or_host
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
                src_url = error_data[CteRucio.REF_PFN_SRC]
                dst_url = error_data[CteRucio.REF_PFN_DST]
                ############ AVOID ERRORS THAT ARE BLACKLISTED ############ç
                in_blacklist = self.is_blacklisted(src_url, dst_url)
                # Extract useful data
                if CteRucio.REF_LOG in error_data.keys() and not in_blacklist:
                    ############ ADD EXTRA DATA ############
                    src_lfn = get_lfn_and_short_pfn(src_url)
                    dst_lfn, _ = get_lfn_and_short_pfn(dst_url)
                    src_protocol = src_url.split("/")[0].strip(":").strip()
                    dst_protocol = dst_url.split("/")[0].strip(":").strip()

                    name = error_data['name']
                    error_data.update({CteRucio.REF_LFN_SRC: src_lfn, CteRucio.REF_LFN_DST: dst_lfn, 'name': name,
                                       "src_protocol": src_protocol, "dst_protocol": dst_protocol})
                    # Clean se
                    error_data[CteRucio.REF_SE_SRC] = error_data[CteRucio.REF_PFN_SRC].split("/")[2]
                    error_data[CteRucio.REF_SE_DST] = error_data[CteRucio.REF_PFN_DST].split("/")[2]
                    ############ GROUP THE ERROR ############
                    grouped_by_error = group_data(grouped_by_error, error_data,
                                                  [CteRucio.REF_PFN_SRC, CteRucio.REF_PFN_DST], CteRucio)
            if grouped_by_error:
                data_host.update({direction: grouped_by_error})
        all_data.update({hostname: data_host})
        return all_data

    def get_column_id(self, num_rows, num_columns_ahead=0, num_rows_ahead=0):
        letter_column = chr(64 + num_columns_ahead)
        structure_id = "{}{}:{}{}".format(letter_column, num_rows_ahead + 1, letter_column,
                                          num_rows + num_rows_ahead + 1)
        return structure_id

    def get_sub_table(self, dict_grouped_by_id, list_elements):
        list_group = []
        for group_id, data in dict_grouped_by_id.items():
            new_row = []
            dict_data = dict((i, data.count(i)) for i in data)
            if dict_data:
                for element_value in list_elements:
                    if element_value in dict_data.keys():
                        new_row.append(dict_data[element_value])
                    else:
                        new_row.append(None)
                list_group.append([group_id] + new_row)
        return list_group

    def write_lfn_txt(self, lfns_file_name, lfns):
        text = ""
        for error, list_lfns in lfns.items():
            text += "*" * 30 + "\n"
            text += error.capitalize() + "\n"
            text += "*" * 30 + "\n"
            for lfn in list_lfns:
                text += lfn + "\n"

        f = open(lfns_file_name + ".txt", "a")
        f.write(text)
        f.close()

    def results_to_csv(self, dict_results, write_lfns=False):
        src_dst_info = ["rse", "type", "se", "url", "lfn", "protocol"]
        expanded_src_dst_info = []
        [expanded_src_dst_info.append(direction + "_" + info) for direction in ["src", "dst"] for info in src_dst_info]
        file_info = ['transfer_id', 'checksum_adler', 'file_size', 'transfer_link',
                     'submitted_at', 'started_at', 'transferred_at', 'purged_reason']

        columns = expanded_src_dst_info + file_info

        ############  ITERATE OVER ALL SRMs HOSTS ############
        for storage_element, se_value in dict_results.items():
            time_analysis = round(time.time())
            host_analysis = "EOEOEO"
            file_name = '{}_{}'.format(time_analysis, host_analysis)
            writer = pd.ExcelWriter(file_name + ".xlsx", engine='xlsxwriter')
            ############ GET DATA ORIGIN AND DESTINATION ############
            for direction, direction_value in se_value.items():
                list_errors, list_groups, list_users, list_other_endpoint = [], [], [], []
                group_id = 1
                users, endpoints, lfns = {}, {}, {}
                if "s" == direction[0]:
                    other_direction = CteRucio.REF_SE_DST
                    other_url_direction = CteRucio.REF_PFN_DST
                    same_url_direction = CteRucio.REF_PFN_SRC
                    same_lfn = CteRucio.REF_LFN_SRC

                else:
                    other_direction = CteRucio.REF_SE_SRC
                    other_url_direction = CteRucio.REF_PFN_SRC
                    same_url_direction = CteRucio.REF_PFN_DST
                    same_lfn = CteRucio.REF_LFN_DST

                ############  ITERATE OVER ALL ERROR GROUPS ############
                for error_key, error_value in direction_value.items():
                    users.update({group_id: []})
                    endpoints.update({group_id: []})
                    lfns.update({error_key: []})
                    failed_transfers = 0
                    ############  ITERATE OVER ALL ERRORS ############
                    for single_error in error_value:
                        # ADD USER IN LIST
                        user_site = self.get_user(single_error[same_url_direction])
                        user_other = self.get_user(single_error[other_url_direction])
                        if user_site:
                            users[group_id] += [user_site] * single_error[CteRucio.REF_NUM_ERRORS]
                            if user_other and user_site != user_other:
                                logging.error("Different users {} vs {}".format(user_site, user_other))
                            if user_site not in list_users:
                                list_users.append(user_site)

                        # ADD ENDPOINT IN LIST
                        other_endpoint = single_error[other_direction]
                        endpoints[group_id] += [other_endpoint] * single_error[CteRucio.REF_NUM_ERRORS]
                        if other_endpoint not in list_other_endpoint:
                            list_other_endpoint.append(other_endpoint)

                        # ADD LIST LFNs
                        if write_lfns and single_error[same_lfn] and single_error[same_lfn] not in lfns[error_key]:
                            lfns[error_key].append(single_error[same_lfn])

                        # ADD ALL THE ERROR INFORMATION
                        values_columns = [single_error[elem] for elem in columns]
                        values_columns.append(user_site)
                        values_columns.append(group_id)
                        # Row errors table
                        list_errors.append(values_columns)
                        # Count total of failed transfers for each group
                        failed_transfers += single_error[CteRucio.REF_NUM_ERRORS]

                    # Row table (legend) group errors
                    list_groups.append([group_id, error_key, len(error_value), failed_transfers])
                    group_id += 1

                # WRITE TXT WITH LFNs
                if write_lfns:
                    lfns_file_name = file_name + "_LFNs_{}".format(direction)
                    self.write_lfn_txt(lfns_file_name, lfns)

                # DF ERRORS
                columns_errors = columns + [CteRucio.REF_USER, "group_id"]
                num_columns_error = len(columns_errors)
                df = pd.DataFrame(list_errors, columns=columns_errors)
                df.to_excel(writer, sheet_name=direction, index=False)
                column_id_error = self.get_column_id(len(list_errors), num_columns_error)

                # DF LEGEND GROUPS
                columns_groups = ["group_id", "error_ref", "num_diff_errors", "num_failed_transfers"]
                start_column = num_columns_error + CteRucio.SEPARATION_COLUMNS
                df_group = pd.DataFrame(list_groups, columns=columns_groups)
                df_group.to_excel(writer, sheet_name=direction, startcol=start_column, index=False)
                column_id_group = self.get_column_id(len(list_groups), start_column + 1)

                # DF USERS
                list_group_users = self.get_sub_table(users, list_users)
                columns_users = ["group_id"] + list_users
                start_column = num_columns_error + CteRucio.SEPARATION_COLUMNS
                start_row_users = len(list_groups) + CteRucio.SEPARATION_ROWS
                if list_group_users:
                    df_users = pd.DataFrame(list_group_users, columns=columns_users)
                    df_users.to_excel(writer, sheet_name=direction, startcol=start_column, startrow=start_row_users,
                                      index=False)

                # DF ENDPOINTS
                list_group_endpoints = self.get_sub_table(endpoints, list_other_endpoint)
                columns_endpoints = ["group_id"] + list_other_endpoint
                start_column = num_columns_error + CteRucio.SEPARATION_COLUMNS
                start_row = start_row_users + len(list_group_users) + CteRucio.SEPARATION_ROWS
                if list_group_endpoints:
                    df_endpoint = pd.DataFrame(list_group_endpoints, columns=columns_endpoints)
                    df_endpoint.to_excel(writer, sheet_name=direction, startcol=start_column, startrow=start_row,
                                         index=False)

                # COLOR SHEET
                worksheet = writer.sheets[direction]
                worksheet.conditional_format(column_id_error, {'type': '3_color_scale'})
                worksheet.conditional_format(column_id_group, {'type': '3_color_scale'})

            writer.save()


class AbstractNLP:
    def __init__(self, raw_text):
        self.raw_text = raw_text
        self.clean_text = self.preprocess_string_nlp(raw_text)

    def preprocess_string_nlp(self, text):
        return text.lower().strip()


class TextNLP(AbstractNLP):
    def __init__(self, raw_text, ref_keywords_clean):
        super().__init__(raw_text)
        self.ref_keywords_clean = ref_keywords_clean

    def get_keyword(self, clean_text):
        keywords_value = None
        keywords = [elem for elem in self.ref_keywords_clean if elem in clean_text]
        # We should not have more than 2 keywords in one error
        if len(keywords) > 1:
            print('ERROR! {}, {}'.format(keywords, clean_text))
            keywords_value = keywords[1]
        if keywords:
            keywords_value = keywords[0]
        return keywords_value


def group_data(grouped_by_error, error_data, list_same_ref, constants):
    ############ ADD EXTRA INFO ############
    error_log = error_data[constants.REF_LOG]
    error_data.update({constants.REF_NUM_ERRORS: 1})

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


def get_lfn_and_short_pfn(raw_pfn):
    lfn, reduced_pfn = "", ""
    if "/store" in raw_pfn:
        lfn = "/store" + raw_pfn.split("/store")[1]
        reduced_pfn = raw_pfn.split("/store")[0]
    return lfn, reduced_pfn


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("hours", help="Number of hours to analyze", type=int)
    parser.add_argument("black", help="Blacklisted elements separated by /", type=str)
    parser.add_argument("site", help="Site/hostname to analyze", type=str)
    parser.add_argument("--write_lfns", help="Write files with all LFNS", action="store_true")
    args = parser.parse_args()
    # print(args.echo)

    time_class = Time(hours=args.hours)
    fts = Transfers(time_class)
    BLACKLIST_PFN = args.black.split("/")
    dict_result = fts.analyze_site(site_or_host=args.site)

    write_lfns = False
    if args.write_lfns:
        write_lfns = True

    fts.results_to_csv(dict_result, write_lfns=write_lfns)

    # time_class = Time(hours=12)
    # fts = Transfers(time_class)
    # BLACKLIST_PFN = ["se3.itep.ru", "se01.indiacms.res.in", "cmsio.rc.ufl.edu"]
    # dict_result = fts.analyze_site(site_or_host="T2_ES_IFCA")
    # fts.results_to_csv(dict_result, write_lfns=False)
