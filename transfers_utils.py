import re
import pandas as pd
import logging
import xlsxwriter


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


class ExcelGenerator:
    def __init__(self, dict_results):
        self.dict_results = dict_results

    @staticmethod
    def get_column_id(num_rows, num_columns_ahead=0, num_rows_ahead=0):
        letter_column = chr(64 + num_columns_ahead)
        structure_id = "{}{}:{}{}".format(letter_column, num_rows_ahead + 1, letter_column,
                                          num_rows + num_rows_ahead + 1)
        return structure_id

    @staticmethod
    def get_sub_table(dict_grouped_by_id, list_elements):
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

    @staticmethod
    def write_lfn_txt(lfns_file_name, lfns):
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

    @staticmethod
    def get_user(url_pfn):
        user = ""
        raw_users = re.findall("/user/(.*)/", str(url_pfn))
        if raw_users:
            user = raw_users[0].split("/")[0].strip("")
            user = user.split(".")[0]  # e.g. gbakas.9c1d054d2d278c14ddc228476ff7559c10393d8d
        if len(raw_users) > 2:
            raise Exception("MULTIPLE USERS ON PFN")
        return user

    def results_to_csv(self, write_lfns=False):
        src_dst_info = ["rse", "type", "se", "url", "lfn", "protocol"]
        expanded_src_dst_info = []
        [expanded_src_dst_info.append(direction + "_" + info) for direction in ["src", "dst"] for info in src_dst_info]
        file_info = ['transfer_id', 'checksum_adler', 'file_size', 'transfer_link',
                     'submitted_at', 'started_at', 'transferred_at', 'purged_reason']

        columns = expanded_src_dst_info + file_info

        ############  ITERATE OVER ALL SRMs HOSTS ############
        for storage_element, se_value in self.dict_results.items():
            time_analysis = round(time.time())
            host_analysis = "EOEOEO"
            file_name = '{}_{}'.format(time_analysis, host_analysis)
            writer = pd.ExcelWriter(file_name + ".xlsx", engine='xlsxwriter')
            ############ GET DATA ORIGIN AND DESTINATION ############
            for direction, direction_value in se_value.items():
                list_errors, list_groups, list_users, list_other_endpoint = [], [], [], []
                users, endpoints, lfns = {}, {}, {}
                group_id = 1
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

