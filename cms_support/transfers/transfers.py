# coding=utf-8

import logging
from utils.constants import CteFTS
import time
from copy import deepcopy
from abc import ABC
import re
from utils.mongotools import MongoDB
import pandas as pd
from utils.query_utils import AbstractQueries, get_lfn_and_short_pfn, group_data, Time, timestamp_to_human_utc
import xlsxwriter


class Transfers(AbstractQueries, ABC):
    def __init__(self, time_class):
        super().__init__(time_class)
        self.index_name = CteFTS.INDEX_ES
        self.index_id = CteFTS.INDEX_ES_ID
        self.mongo = MongoDB()

    def get_mongo_query(self, site="", hostname=""):
        ############ GET SRM ELEMENTS OF THE SITE ############
        mongo_query = {CteFTS.REF_FLAVOUR: "SRM"}
        if site:
            mongo_query.update({CteFTS.REF_SITE: site})
        if hostname:
            mongo_query.update({CteFTS.REF_HOST: hostname})
        return mongo_query

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

    def build_query_get_response(self, hostname, direction="", filter_error_kibana=""):
        """

        :param hostname: name of the host to analyze e.g. eoscmsftp.cern.ch
        :param direction: source_se or dest_se
        :param filter_error_kibana: keyword of the error to find e.g. "No such file"
        :return:
        """
        ############ CONSTRUCT THE QUERY ############
        kibana_query_failed = "data.vo:cms AND data.file_state:{} AND data.{}:/.*{}.*/ ".format("FAILED", direction,
                                                                                                hostname)
        # If an error is specified --> add filter on the query
        if filter_error_kibana:
            kibana_query_failed += " AND data.{}:/.*\"{}\".*/".format(CteFTS.REF_LOG, filter_error_kibana)

        ############ QUERY TO ELASTICSEARCH ############
        response_failed = self.get_direct_response(kibana_query=kibana_query_failed, max_results=10000)
        return response_failed

    def analyze_site(self, site="", hostname="", filter_error_kibana=""):
        """
        Get json with all the errors grouped by: host, destination/origin, type of error
        """
        all_data = {}
        mongo_query = self.get_mongo_query(site, hostname)
        list_site_info = self.mongo.find_document(self.mongo.vofeed, mongo_query)
        if list_site_info:
            hosts_name = [info[CteFTS.REF_HOST] for info in list_site_info if info]
            ############  ITERATE OVER ALL SRMs HOSTS ############
            for hostname in hosts_name:
                data_host = {}
                ############ GET DATA ORIGIN AND DESTINATION ############
                for direction in [CteFTS.REF_SE_SRC, CteFTS.REF_SE_DST]:
                    time.sleep(0.1)
                    response_kibana = self.build_query_get_response(hostname, direction=direction,
                                                                    filter_error_kibana=filter_error_kibana)
                    ############ GROUP DATA BY ERRORS ############
                    grouped_by_error = {}
                    ############ ITERATE OVER ALL ERRORS ############
                    for error in response_kibana:
                        error_data = deepcopy(error[CteFTS.REF_DATA])
                        src_url = error_data[CteFTS.REF_PFN_SRC]
                        dst_url = error_data[CteFTS.REF_PFN_DST]
                        ############ AVOID ERRORS THAT ARE BLACKLISTED ############ç
                        in_blacklist = self.is_blacklisted(src_url, dst_url)
                        # Extract useful data
                        if CteFTS.REF_LOG in error_data.keys() and not in_blacklist:
                            ############ ADD EXTRA DATA ############
                            src_lfn, _ = get_lfn_and_short_pfn(src_url)
                            dst_lfn, _ = get_lfn_and_short_pfn(dst_url)
                            timestamp_hr = timestamp_to_human_utc(error_data[CteFTS.REF_TIMESTAMP])
                            error_data.update({CteFTS.REF_LFN_SRC: src_lfn, CteFTS.REF_LFN_DST: dst_lfn,
                                               CteFTS.REF_TIMESTAMP_HR: timestamp_hr})
                            # Clean se
                            error_data[CteFTS.REF_SE_SRC] = error_data[CteFTS.REF_SE_SRC].split("/")[-1]
                            error_data[CteFTS.REF_SE_DST] = error_data[CteFTS.REF_SE_DST].split("/")[-1]
                            ############ GROUP THE ERROR ############
                            grouped_by_error = group_data(grouped_by_error, error_data,
                                                          [CteFTS.REF_PFN_SRC, CteFTS.REF_PFN_DST], CteFTS)
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

        columns = [CteFTS.REF_TIMESTAMP, CteFTS.REF_TIMESTAMP_HR, CteFTS.REF_LOG,
                   CteFTS.REF_SE_SRC, CteFTS.REF_SE_DST, CteFTS.REF_PFN_SRC, CteFTS.REF_PFN_DST, CteFTS.REF_LFN_SRC,
                   CteFTS.REF_LFN_DST, CteFTS.REF_NUM_ERRORS, CteFTS.REF_JOB_ID, CteFTS.REF_FILE_ID]

        ############  ITERATE OVER ALL SRMs HOSTS ############
        for storage_element, se_value in dict_results.items():
            time_analysis = round(time.time())
            host_analysis = storage_element.replace(".", "-")
            file_name = '{}_{}'.format(time_analysis, host_analysis)
            writer = pd.ExcelWriter(file_name + ".xlsx", engine='xlsxwriter')
            ############ GET DATA ORIGIN AND DESTINATION ############
            for direction, direction_value in se_value.items():
                list_errors, list_groups, list_users, list_other_endpoint = [], [], [], []
                group_id = 1
                users, endpoints, lfns = {}, {}, {}
                if "s" == direction[0]:
                    other_direction = CteFTS.REF_SE_DST
                    other_url_direction = CteFTS.REF_PFN_DST
                    other_lfn = CteFTS.REF_LFN_DST
                    same_url_direction = CteFTS.REF_PFN_SRC
                    same_lfn = CteFTS.REF_LFN_SRC

                else:
                    other_direction = CteFTS.REF_SE_SRC
                    other_url_direction = CteFTS.REF_PFN_SRC
                    same_url_direction = CteFTS.REF_PFN_DST
                    other_lfn = CteFTS.REF_LFN_SRC
                    same_lfn = CteFTS.REF_LFN_DST

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
                            users[group_id] += [user_site] * single_error[CteFTS.REF_NUM_ERRORS]
                            if user_other and user_site != user_other:
                                logging.error("Different users {} vs {}".format(user_site, user_other))
                            if user_site not in list_users:
                                list_users.append(user_site)

                        # ADD ENDPOINT IN LIST
                        other_endpoint = single_error[other_direction]
                        endpoints[group_id] += [other_endpoint] * single_error[CteFTS.REF_NUM_ERRORS]
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
                        failed_transfers += single_error[CteFTS.REF_NUM_ERRORS]

                    # Row table (legend) group errors
                    list_groups.append([group_id, error_key, len(error_value), failed_transfers])
                    group_id += 1

                # WRITE TXT WITH LFNs
                if write_lfns:
                    lfns_file_name = file_name + "_LFNs_{}".format(direction)
                    self.write_lfn_txt(lfns_file_name, lfns)

                # DF ERRORS
                columns_errors = columns + [CteFTS.REF_USER, "group_id"]
                num_columns_error = len(columns_errors)
                df = pd.DataFrame(list_errors, columns=columns_errors)
                df.to_excel(writer, sheet_name=direction, index=False)
                column_id_error = self.get_column_id(len(list_errors), num_columns_error)

                # DF LEGEND GROUPS
                columns_groups = ["group_id", "error_ref", "num_diff_errors", "num_failed_transfers"]
                start_column = num_columns_error + CteFTS.SEPARATION_COLUMNS
                df_group = pd.DataFrame(list_groups, columns=columns_groups)
                df_group.to_excel(writer, sheet_name=direction, startcol=start_column, index=False)
                column_id_group = self.get_column_id(len(list_groups), start_column + 1)

                # DF USERS
                list_group_users = self.get_sub_table(users, list_users)
                columns_users = ["group_id"] + list_users
                start_column = num_columns_error + CteFTS.SEPARATION_COLUMNS
                start_row_users = len(list_groups) + CteFTS.SEPARATION_ROWS
                if list_group_users:
                    df_users = pd.DataFrame(list_group_users, columns=columns_users)
                    df_users.to_excel(writer, sheet_name=direction, startcol=start_column, startrow=start_row_users,
                                      index=False)

                # DF ENDPOINTS
                list_group_endpoints = self.get_sub_table(endpoints, list_other_endpoint)
                columns_endpoints = ["group_id"] + list_other_endpoint
                start_column = num_columns_error + CteFTS.SEPARATION_COLUMNS
                start_row = start_row_users + len(list_group_users) + CteFTS.SEPARATION_ROWS
                if list_group_endpoints:
                    df_endpoint = pd.DataFrame(list_group_endpoints, columns=columns_endpoints)
                    df_endpoint.to_excel(writer, sheet_name=direction, startcol=start_column, startrow=start_row,
                                         index=False)

                # COLOR SHEET
                worksheet = writer.sheets[direction]
                worksheet.conditional_format(column_id_error, {'type': '3_color_scale'})
                worksheet.conditional_format(column_id_group, {'type': '3_color_scale'})

            writer.save()


if __name__ == "__main__":
    time_class = Time(hours=24)
    fts = Transfers(time_class)
    BLACKLIST_PFN = ["se3.itep.ru", "se01.indiacms.res.in", "cmsio.rc.ufl.edu"]
    dict_result = fts.analyze_site(site="T2_ES_IFCA")
    fts.results_to_csv(dict_result, write_lfns=True)






























