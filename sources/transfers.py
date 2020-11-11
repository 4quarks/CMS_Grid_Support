from query_utils import *
import spacy
import pandas as pd

NLP = spacy.load("en_core_web_lg")

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
    return Cte.AND + attr + Cte.EXPRESSION.format(Cte.INCLUDED.format(expression))

def get_query(self, dst_url="", dst_protocol="", src_url="", src_protocol="", status="", reason=""):
    raw_query = "data.vo:cms"
    if dst_url:
        raw_query += self.join_attribute_and_expression(self.attr_destination, dst_url)
    if status:
        raw_query += Cte.AND + self.attr_status + status

    clean_str_query = get_str_lucene_query(self.index['name'], self.time_slot[0], self.time_slot[1], raw_query)
    return clean_str_query

https://monit-kibana.cern.ch/kibana/goto/31129642d019649a5fe8ee3c816677e6
"""


class Transfers(AbstractQueries, ABC):
    def __init__(self, time_class):
        super().__init__(time_class)
        self.index_name = "monit_prod_fts_raw_*"
        self.index_id = "9233"
        self.mongo = MongoDB()

    def preprocess_string_nlp(self, string):
        return string.lower().strip()

    def get_keyword(self, string):
        keywords_value = None
        keywords = [elem for elem in Cte.KEYWORDS_ERRORS if elem in string]
        # We should not have more than 2 keywords in one error
        if len(keywords) > 1:
            raise Exception('My error! {}, {}'.format(keywords, string))
        if keywords:
            keywords_value = keywords[0]
        return keywords_value

    def value_in_list(self, error_log, previous_errors, strict_comparison=False):
        in_list = False
        similar_error = error_log
        keyword = ""
        # DIRECT COMPARISON
        if strict_comparison:
            if error_log in previous_errors:
                in_list = True
        # USE NLP TO MATCH KEYBOARDS AND OTHER ERRORS
        else:
            clean_error = self.preprocess_string_nlp(error_log)
            nlp_error = NLP(clean_error)
            keyword_error = self.get_keyword(clean_error)
            keyword = keyword_error
            # Check if the current error can be grouped with any other previous error
            for previous_error in previous_errors:
                if previous_error:
                    clean_previous_error = self.preprocess_string_nlp(previous_error)
                    keywords_value_list = self.get_keyword(clean_previous_error)

                    if keyword_error == keywords_value_list:
                        in_list = True
                        similar_error = previous_error
                        break

                    nlp_previous_error = NLP(clean_previous_error)  # Finding the similarity
                    score = nlp_error.similarity(nlp_previous_error)

                    # IF THEY ARE SIMILAR (NLP) OR THEY CONTAIN THE SAME KEYBOARD --> SAME ISSUE
                    if score > Cte.THRESHOLD_NLP:
                        in_list = True
                        similar_error = previous_error
                        break

        return in_list, similar_error, keyword

    def is_blacklisted(self, src_url, dest_url):
        """
        Check if the pfn contains blacklisted elements
        :param src_url: 'gsiftp://eoscmsftp.cern.ch//eos/cms/store/temp/user/cc/.../out_offline_Photons_230.root'
        :param dest_url:
        :return:
        """
        black_pfn = [black_pfn for black_pfn in BLACKLIST_PFN if black_pfn in src_url or black_pfn in dest_url]
        return black_pfn

    def group_data(self, grouped_by_error, error):
        error_data = deepcopy(error["data"])
        ############ DETAILED DATA OF THE ERROR ############
        error_log = error_data[Cte.REF_ERROR_LOG]
        src_url = error_data[Cte.REF_PFN_SRC]
        dst_url = error_data[Cte.REF_PFN_DST]
        timestamp_hr = timestamp_to_human_utc(error_data[Cte.REF_TIMESTAMP])
        # Clean se
        error_data[Cte.REF_SE_SRC] = error_data[Cte.REF_SE_SRC].split("/")[-1]
        error_data[Cte.REF_SE_DST] = error_data[Cte.REF_SE_DST].split("/")[-1]

        ############ ADD EXTRA DATA ############
        error_data.update({Cte.REF_NUM_ERRORS: 1, Cte.REF_TIMESTAMP_HR: timestamp_hr})

        ############ AVOID ERRORS THAT ARE BLACKLISTED ############
        in_blacklist = self.is_blacklisted(src_url, dst_url)
        if error_log and not in_blacklist:
            error_in_list, similar_error, keyword = self.value_in_list(error_log, list(grouped_by_error.keys()))
            ############ CHOOSE REFERENCE ############
            if not keyword:
                # The reference is the error log
                print('different error: ', similar_error)
                error_ref = similar_error
            else:
                # The reference is the keyword
                error_ref = keyword
            if not error_in_list:
                # If the error was not grouped --> add
                grouped_by_error.update({error_ref: [error_data]})
            else:
                # If the error match with another --> count repetition
                matched_element = False
                # EXACT SAME ERROR REPEATED --> num_errors + 1
                for idx, element in enumerate(grouped_by_error[error_ref]):
                    # SAME ERROR & SAME PFN --> SAME ISSUE
                    if element[Cte.REF_PFN_SRC] == src_url and element[Cte.REF_PFN_DST] == dst_url:
                        grouped_by_error[error_ref][idx][Cte.REF_NUM_ERRORS] += 1
                        matched_element = True
                        break
                if not matched_element:
                    grouped_by_error[error_ref].append(error_data)
        return grouped_by_error

    def get_user(self, url_pfn):
        user = ""
        raw_users = re.findall("/user/(.*)/", str(url_pfn))
        if raw_users:
            user = raw_users[0].split("/")[0].strip("")
        if len(raw_users) > 2:
            raise Exception("MULTIPLE USERS ON PFN")
        return user

    def group_by_error(self, response_clean):
        """
        Get all errors and the source/destination PFNs

        :param response_clean: output --> get_response
        :return: dict with each error found and the list of pfns (from/to) and lfns
        e.g.
        {"Source and destination file size mismatch":
            [
                {"dst": "gsiftp://se.cis.gov.pl:2811//dpm/cis.gov.pl/home/cms",
                "src": "srm://srm-cms.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/cms",
                "lfn": "/store/PhEDEx_LoadTest07_4/LoadTest07_CERN_196/.../file.root"}, ...
            ]
        """
        grouped_by_error = {}
        ############ ITERATE OVER ALL ERRORS ############
        for error in response_clean:
            # Extract useful data
            if "reason" in error["data"].keys():
                ############ GROUP THE ERROR ############
                grouped_by_error = self.group_data(grouped_by_error, error)
        return grouped_by_error

    def get_data_from_to_host(self, hostname, direction="", error=""):
        """

        :param hostname: name of the host to analyze e.g. eoscmsftp.cern.ch
        :param direction: source_se or dest_se
        :param error: keyword of the error to find e.g. "No such file"
        :return: json with all the information of the errors
        """

        ############ CONSTRUCT THE QUERY ############
        kibana_query_failed = "data.vo:cms AND data.file_state:{} AND data.{}:/.*{}.*/ ".format("FAILED", direction,
                                                                                                hostname)
        # If an error is specified --> add filter on the query
        if error:
            kibana_query_failed += " AND data.reason:/.*\"{}\".*/".format(error)

        ############ QUERY TO ELASTICSEARCH ############
        response_failed = self.get_direct_response(kibana_query=kibana_query_failed, max_results=2000)

        ############ GROUP DATA BY ERRORS ############
        grouped_by_error = self.group_by_error(response_failed)

        return grouped_by_error

    def analyze_site(self, site_name="", hostname="", error=""):
        """
        Get json with all the errors grouped by: host, destination/origin, type of error
        :param site_name:
        :param hostname:
        :param error:
        :return:
        """
        all_data = {}
        if not hostname and site_name:
            ############ GET SRM ELEMENTS OF THE SITE ############
            list_site_info = self.mongo.find_document(self.mongo.vofeed, {"site": site_name, "flavour": "SRM"})
            if list_site_info:
                hosts_name = [info["hostname"] for info in list_site_info if info]
                ############  ITERATE OVER ALL SRMs HOSTS ############
                for hostname in hosts_name:
                    data_host = {}
                    ############ GET DATA ORIGIN AND DESTINATION ############
                    for direction in [Cte.REF_SE_SRC, Cte.REF_SE_DST]:
                        time.sleep(0.1)
                        direction_data = self.get_data_from_to_host(hostname, direction=direction, error=error)
                        if direction_data:
                            data_host.update({direction: direction_data})
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


    def results_to_csv(self, json_results):
        """
        :param json_results:
        :return:
        """
        SEPARATION_COLUMNS = 2
        SEPARATION_ROWS = 4
        columns = [Cte.REF_TIMESTAMP, Cte.REF_TIMESTAMP_HR, Cte.REF_ERROR_LOG,
                   Cte.REF_SE_SRC, Cte.REF_SE_DST, Cte.REF_PFN_SRC, Cte.REF_PFN_DST, Cte.REF_NUM_ERRORS, "job_id",
                   "file_id"]
        ############  ITERATE OVER ALL SRMs HOSTS ############
        for storage_element, se_value in json_results.items():
            file_name = '{}_{}.xlsx'.format(round(time.time()), storage_element.replace(".", "-"))
            writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
            ############ GET DATA ORIGIN AND DESTINATION ############
            for direction, direction_value in se_value.items():
                list_errors, list_groups, list_users, list_other_endpoint = [], [], [], []
                group_id = 1
                users, endpoints = {}, {}
                other_direction = [elem for elem in se_value.keys() if elem != direction][0]
                ############  ITERATE OVER ALL ERROR GROUPS ############
                for error_key, error_value in direction_value.items():
                    users.update({group_id: []})
                    endpoints.update({group_id: []})
                    failed_transfers = 0
                    ############  ITERATE OVER ALL ERRORS ############
                    for single_error in error_value:

                        # ADD USER IN LIST
                        user = self.get_user(single_error[Cte.REF_PFN_DST])
                        if user:
                            users[group_id] += [user]*single_error[Cte.REF_NUM_ERRORS]
                        if user not in list_users:
                            list_users.append(user)

                        # ADD ENDPOINT IN LIST
                        other_endpoint = single_error[other_direction]
                        endpoints[group_id] += [other_endpoint] * single_error[Cte.REF_NUM_ERRORS]
                        if other_endpoint not in list_other_endpoint:
                            list_other_endpoint.append(other_endpoint)

                        # ADD ALL THE ERROR INFORMATION
                        values_columns = [single_error[elem] for elem in columns]
                        values_columns.append(user)
                        values_columns.append(group_id)
                        # Row errors table
                        list_errors.append(values_columns)
                        # Count total of failed transfers for each group
                        failed_transfers += single_error[Cte.REF_NUM_ERRORS]

                    # Row table (legend) group errors
                    list_groups.append([group_id, error_key, len(error_value), failed_transfers])
                    group_id += 1

                # DF ERRORS
                columns_errors = columns + ["user", "group_id"]
                num_columns_error = len(columns_errors)
                df = pd.DataFrame(list_errors, columns=columns_errors)
                df.to_excel(writer, sheet_name=direction, index=False)
                column_id_error = self.get_column_id(len(list_errors), num_columns_error)

                # DF LEGEND GROUPS
                columns_groups = ["group_id", "error_ref", "num_diff_errors", "num_failed_transfers"]
                start_column = num_columns_error + SEPARATION_COLUMNS
                df_group = pd.DataFrame(list_groups, columns=columns_groups)
                df_group.to_excel(writer, sheet_name=direction, startcol=start_column, index=False)
                column_id_group = self.get_column_id(len(list_groups), start_column + 1)

                # DF USERS
                list_group_users = self.get_sub_table(users, list_users)
                columns_users = ["group_id"] + list_users
                start_column = num_columns_error + SEPARATION_COLUMNS
                start_row_users = len(list_groups) + SEPARATION_ROWS
                if list_group_users:
                    df_users = pd.DataFrame(list_group_users, columns=columns_users)
                    df_users.to_excel(writer, sheet_name=direction, startcol=start_column, startrow=start_row_users,
                                      index=False)

                # DF ENDPOINTS
                list_group_endpoints = self.get_sub_table(endpoints, list_other_endpoint)
                columns_endpoints = ["group_id"] + list_other_endpoint
                start_column = num_columns_error + SEPARATION_COLUMNS
                start_row = start_row_users + len(list_group_users) + SEPARATION_ROWS
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
    # a = fts.analyze_site(hostname="srm-cms.gridpp.rl.ac.uk")
    BLACKLIST_PFN = ["se3.itep.ru", "LoadTest", "se01.indiacms.res.in"]
    # dict_result = fts.analyze_site(site_name="T2_US_UCSD")  # , error="CHECKSUM")
    # with open('all.json', 'w') as outfile:
    #     json.dump(dict_result, outfile)

    with open('all.json') as outfile:
        data = json.load(outfile)
    fts.results_to_csv(data)
    # error = "User timeout over"

    # kibana_query = "data.vo:cms AND source_se:/.*storm.ifca.es.*/ AND data.reason:/.*\"{}\".*/".format(error)
    # kibana_query = "data.vo:cms  AND data.source_se:/.*gftp.t2.ucsd.edu.*/ AND data.file_state:FAILED " \
    #                "AND data.reason:/.*CHECKSUM.*|.*No such file.*/"

    # endpoint = "srm-cms.gridpp.rl.ac.uk"
    # # reason = "AND data.reason:/.*\"User timeout over\".*/"
    # reason = ""
    # kibana_query = "data.vo:cms AND data.file_state:FAILED AND " \
    #                "(data.source_se:/.*{}.*/ OR data.dest_se:/.*{}.*/) {}".format(endpoint, endpoint, reason)
    # # kibana_query = "data.vo:cms  AND data.dest_se:/.*eoscmsftp.cern.ch.*/ AND data.file_state:FAILED"
