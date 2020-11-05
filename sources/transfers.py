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

    def create_str_link(self, origin, destination):
        return str(origin) + '__' + destination

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
            # Check if the current error can be grouped with any other previous error
            for previous_error in previous_errors:
                if previous_error:
                    clean_previous_error = self.preprocess_string_nlp(previous_error)
                    nlp_previous_error = NLP(clean_previous_error)  # Finding the similarity
                    score = nlp_error.similarity(nlp_previous_error)
                    keywords_value_list = self.get_keyword(clean_previous_error)

                    # IF THEY ARE SIMILAR (NLP) OR THEY CONTAIN THE SAME KEYBOARD --> SAME ISSUE
                    if score > Cte.THRESHOLD_NLP or keyword_error == keywords_value_list:
                        in_list = True
                        similar_error = previous_error
                        break
        return in_list, similar_error, keyword

    def group_data(self, grouped_by_error, min_dict, error_log):
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
            grouped_by_error.update({error_ref: [min_dict]})
        else:
            # If the error match with another --> count repetition
            matched_element = False
            # EXACT SAME ERROR REPEATED --> num_errors + 1
            for idx, element in enumerate(grouped_by_error[error_ref]):
                # SAME ERROR & SAME PFN --> SAME ISSUE
                if element['pfn_from'] == min_dict['pfn_from'] and element['pfn_to'] == min_dict['pfn_to']:
                    grouped_by_error[error_ref][idx]['num_errors'] = grouped_by_error[error_ref][idx]['num_errors'] + 1
                    matched_element = True
                    break

            if not matched_element:
                grouped_by_error[error_ref].append(min_dict)
        return grouped_by_error

    def get_user(self, error_log):
        """
        :param error_log: Disk quota exceeded
        return: the list of the users with this issue
        """
        users = []
        raw_users = re.findall("/user/(.*)/", str(error_log))
        for user in raw_users:
            clean_user = user.split("/")[0].strip("")
            users.append(clean_user)
        return unique_elem_list(users)

    def get_grouped_errors(self, response_clean):
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
        list_users = []
        for error in response_clean:
            # Extract useful data
            if "reason" in error["data"].keys():
                error_log = error["data"]["reason"].strip("")

                # Get users
                user = self.get_user(error_log)
                if user:
                    list_users.append(user)

                # Source
                pfn_src = error["data"]["src_url"].strip("")
                source = error["data"]["source_se"].split('://')[1].strip("")
                # Destination
                pfn_dst = error["data"]["dst_url"]
                destination = error["data"]["dest_se"].split('://')[1].strip("")
                timestamp = error["data"]["timestamp"]

                # LFN
                lfn, _ = get_lfn_and_short_pfn(pfn_src)

                ############ DETAILED DATA OF THE ERROR ############
                min_data = {'pfn_from': pfn_src, 'pfn_to': pfn_dst, 'num_errors': 1,
                            'error': error_log, "timestamp": timestamp, 'lfn': lfn,
                            "timestamp_hr": timestamp_to_human_utc(timestamp)}

                ############ GROUP THE ERROR ############
                in_blacklist = source in BLACKLIST_HOSTS or destination in BLACKLIST_HOSTS or \
                               BLACKLIST_PFN in pfn_src or BLACKLIST_PFN in pfn_dst
                if error_log and not in_blacklist:
                    min_error = deepcopy(min_data)
                    min_error.update({'se_from': source, 'se_to': destination})
                    grouped_by_error = self.group_data(grouped_by_error, min_error, error_log)

        return grouped_by_error

    def get_lfn_per_error(self, grouped_by_error):
        """
        Get LFNs with the error:
        :param response_grouped: output --> get_grouped_errors
        :return: dict with all errors and each lfn with the number of occurrences.
        e.g.
        {"Source and destination file size mismatch":
            {
                "/store/PhEDEx_LoadTest07_4/LoadTest07_CERN_196": 1,
                "/store/PhEDEx_LoadTest07/LoadTest07_Prod_Caltech/LoadTest07_Caltech_4E": 6,
                ...
            },
            ...
        }
        """
        all_errors = []
        all_list_lfn = []
        for error, list_min_data in grouped_by_error.items():
            list_lfn = []
            for min_data in list_min_data:
                lfn, _ = get_lfn_and_short_pfn(min_data['pfn_from'])
                list_lfn.append(lfn)
                all_list_lfn.append(lfn)
            all_errors.append({error: unique_elem_list(list_lfn)})
        return all_errors, unique_elem_list(all_list_lfn)

    def get_error_per_type(self, grouped_by_error):
        """
        Count the number of errors

        :param response_grouped: output --> get_grouped_errors
        :return: dict with each error reason and the number of occurrences
        e.g.
        {
            "DESTINATION [22] Source and destination file size mismatch": 1,
            "DESTINATION [17] Destination file exists and overwrite is not enabled": 3,
            ...
        }
        """
        return count_repeated_elements_list(list(grouped_by_error.keys()))

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
        grouped_by_error = self.get_grouped_errors(response_failed)

        return grouped_by_error

    def analyze_site(self, site_name="", hostname="", error=""):
        """

        :param site_name:
        :param hostname:
        :param error:
        :return:
        """
        all_data = {}
        if not hostname and site_name:
            list_site_info = self.mongo.find_document(self.mongo.vofeed, {"site": site_name, "flavour": "SRM"})
            if list_site_info:
                hosts_name = [info["hostname"] for info in list_site_info if info]
                # ITERATE OVER ALL SRMs OF THE SITE
                for hostname in hosts_name:
                    data_host = {}
                    # ORIGIN AND DESTINATION
                    for direction in ["source_se", "dest_se"]:
                        time.sleep(0.1)
                        direction_data = self.get_data_from_to_host(hostname, direction=direction, error=error)
                        if direction_data:
                            data_host.update({direction: direction_data})
                    all_data.update({hostname: data_host})
        return all_data

    def results_to_csv(self, json_results):
        """
        :param json_results:
        :return:
        """
        columns = ["timestamp", "timestamp_hr", "error", "se_from",
                   "se_to", "pfn_from", "pfn_to", "num_errors"]

        for storage_element, se_value in json_results.items():
            file_name = '{}.xlsx'.format(storage_element.replace(".", "-"))
            writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
            for direction, direction_value in se_value.items():
                list_errors, list_groups = [], []
                group_id = 1
                for error_key, error_value in direction_value["grouped_by_error"].items():
                    failed_transfers = 0
                    for single_error in error_value:
                        values_columns = [single_error[elem] for elem in columns]
                        values_columns.append(group_id)

                        list_errors.append(values_columns)

                        failed_transfers += single_error["num_errors"]
                    list_groups.append([group_id, error_key, len(error_value), failed_transfers])
                    group_id += 1

                # DF ERRORS
                df = pd.DataFrame(list_errors, columns=columns + ["group_id"])
                df.to_excel(writer, sheet_name=direction, index=False)

                # DF LEGEND GROUPS
                df_group = pd.DataFrame(list_groups, columns=["group_id", "error_ref", "num_diff_errors",
                                                              "num_failed_transfers"])
                df_group.to_excel(writer, sheet_name=direction, startcol=12, index=False)

                # COLOR SHEET
                worksheet = writer.sheets[direction]
                column_id_error = "I1:I{}".format(str(len(list_errors) + 1))
                worksheet.conditional_format(column_id_error, {'type': '3_color_scale'})
                column_id_group = "M1:M{}".format(str(len(list_groups) + 1))
                worksheet.conditional_format(column_id_group, {'type': '3_color_scale'})

            writer.save()


if __name__ == "__main__":
    time_class = Time(hours=24)
    fts = Transfers(time_class)
    # a = fts.analyze_site(hostname="srm-cms.gridpp.rl.ac.uk")
    BLACKLIST_HOSTS = ["se01.indiacms.res.in", "se3.itep.ru"]
    BLACKLIST_PFN = "LoadTest"
    dict_result = fts.analyze_site(site_name="T2_CH_CERN")  # , error="CHECKSUM")
    with open('all.json', 'w') as outfile:
        json.dump(dict_result, outfile)

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
