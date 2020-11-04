from query_utils import *
import spacy
import pandas as pd
import xlsxwriter

NLP = spacy.load("en_core_web_lg")
THRESHOLD = 0.99
KEYWORDS_ERRORS = ['srm_authorization_failure', 'overwrite is not enabled', 'internal server error',
                   'no such file or directory', 'timeout of 360 seconds has been exceeded',
                   'connection refused globus_xio', 'checksum',
                   'source and destination file size mismatch', 'protocol family not supported', 'permission_denied',
                   'srm_putdone error on the surl', 'srm_get_turl error on the turl', 'no route to host',
                   "an end of file occurred", "stream ended before eod", 'handle not in the proper state',
                   'unable to get quota space', 'received block with unknown descriptor', 'no data available',
                   'file has no copy on tape and no diskcopies are accessible', 'valid credentials could not be found',
                   'a system call failed: connection timed out', 'operation timed out', 'user timeout over',
                   'idle timeout: closing control connection', 'system error in write into hdfs',
                   'reports could not open connection to', 'closing xrootd file handle']

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

# class TransferAttributes:
#     attr_destination = "data.dst_url:"
#     attr_source = "data.src_url:"
#     attr_status = "data.file_state:"
#     attr_reason = "data.reason:"

#
# class Status:
#     error = "FAILED"
#     active = "ACTIVE"
#     ready = "READY"
#     submitted = "SUBMITTED"
#     finished = "FINISHED"

# def join_attribute_and_expression(self, attr, expression):
#     return Cte.AND + attr + Cte.EXPRESSION.format(Cte.INCLUDED.format(expression))

# def get_query(self, dst_url="", dst_protocol="", src_url="", src_protocol="", status="", reason=""):
#     raw_query = "data.vo:cms"
#     if dst_url:
#         raw_query += self.join_attribute_and_expression(self.attr_destination, dst_url)
#     if status:
#         raw_query += Cte.AND + self.attr_status + status
# 
#     clean_str_query = get_str_lucene_query(self.index['name'], self.time_slot[0], self.time_slot[1], raw_query)
#     return clean_str_query

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
        keywords = [elem for elem in KEYWORDS_ERRORS if elem in string]
        if len(keywords) > 1:
            raise Exception('My error! {}, {}'.format(keywords, string))
        if not keywords:
            print()
        return keywords

    def value_in_list(self, value, list_values, strict_comparison=False):
        in_list = False
        similar_value = value
        keyword = ""

        # DIRECT COMPARISON
        if strict_comparison:
            if value in list_values:
                in_list = True
        # USE NLP TO MATCH KEYBOARDS AND OTHER ERRORS
        else:
            clean_value = self.preprocess_string_nlp(value)
            doc = NLP(clean_value)
            keywords_value = self.get_keyword(clean_value)
            # TODO
            if keywords_value:
                keyword = keywords_value[0]
            for value_list in list_values:
                if value_list:
                    clean_value_list = self.preprocess_string_nlp(value_list)
                    doc1 = NLP(clean_value_list)  # Finding the similarity
                    score = doc.similarity(doc1)
                    keywords_value_list = self.get_keyword(clean_value_list)
                    keywords_match = list(set(keywords_value) & set(keywords_value_list))
                    if score > THRESHOLD or keywords_match:
                        in_list = True
                        similar_value = value_list
                        break
        return in_list, similar_value, keyword

    def group_data(self, grouped_by_error, min_dict, error_log):
        # value_in_list, similar_value = self.value_in_list(group_ref,  list(grouped_all.keys()), strict_comparison)
        error_in_list, similar_error, keyword = self.value_in_list(error_log, list(grouped_by_error.keys()))

        if not keyword:
            print('different error: ', similar_error)
            error_ref = similar_error
        else:
            error_ref = keyword

        if not error_in_list:
            grouped_by_error.update({error_ref: [min_dict]})
        else:
            matched_element = False
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
        """
        users = []
        raw_users = re.findall("/user/(.*)/", str(error_log))
        for user in raw_users:
            clean_user = user.split("/")[0].strip("")
            # if clean_user == "ddirmait":
            #     print()
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
        grouped_by_site = {}
        list_users = []
        errors_grouped_nlp = {}
        for error in response_clean:
            # Extract useful data
            if "reason" in error["data"].keys():
                error_log = error["data"]["reason"].strip("")
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

                min_data = {'pfn_from': pfn_src, 'pfn_to': pfn_dst, 'num_errors': 1,
                            'error': error_log, "timestamp": timestamp,
                            "timestamp_hr": timestamp_to_human_utc(timestamp)}

                # Append Error data
                if error_log:
                    min_error = deepcopy(min_data)
                    min_error.update({'se_from': source, 'se_to': destination})
                    grouped_by_error = self.group_data(grouped_by_error, min_error, error_log)

                    # Append site data
                    # min_sites = deepcopy(min_data)
                    # link = self.create_str_link(source, destination)
                    # link_in_list, similar_link, _ = self.value_in_list(link, list(grouped_by_site.keys()),
                    #                                                    strict_comparison=True)
                    # grouped_by_site = self.group_data(grouped_by_site, min_sites, similar_link, link_in_list)

        # print("users: ", unique_elem_list(sum(list_users, [])))
        """
        # TODO: change keywords
        for key_error, list_errors in errors_grouped_nlp.items():
            diff = []
            for c in list_errors:
                common_words = unique_elem_list(c.split()+key_error.split())
                different_words = list(set(common_words) - set(c.split()))
                diff.append(different_words)
            print()
        print(json.dumps(errors_grouped_nlp))
        """
        return grouped_by_error, grouped_by_site

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

    def get_data_from_to_host(self, hostname, direction="source_se", error=""):
        kibana_query_ok = "data.vo:cms AND data.file_state:{} AND data.{}:/.*{}.*/ ".format("FINISHED", direction,
                                                                                            hostname)
        kibana_query_failed = "data.vo:cms AND data.file_state:{} AND data.{}:/.*{}.*/ ".format("FAILED", direction,
                                                                                                hostname)

        if error:
            kibana_query_failed += " AND data.reason:/.*\"{}\".*/".format(error)
            response_failed = self.get_direct_response(kibana_query=kibana_query_failed, max_results=2000)
        else:
            response_failed = self.get_direct_response(kibana_query=kibana_query_failed, max_results=2000)
            # response_ok = self.get_direct_response(kibana_query=kibana_query_ok, max_results=2000)
            # num_failed = len(response_failed)
            # num_ok = len(response_ok)
            # quality = round(num_ok / num_failed, 3)

        grouped_by_error, grouped_by_site = self.get_grouped_errors(response_failed)
        lfn_errors, _ = self.get_lfn_per_error(grouped_by_error)
        host_data = {}
        if grouped_by_error:
            host_data = {"time_range_UTC": self.time_class.time_slot_hr,
                         "grouped_by_error": grouped_by_error,
                         "list_lfn": lfn_errors,
                         "num_failed_transfers": len(response_failed)}
        return host_data

    def analyze_site(self, site_name="", hostname="", error=""):
        all_data = {}
        if not hostname and site_name:
            list_site_info = self.mongo.find_document(self.mongo.vofeed, {"site": site_name, "flavour": "SRM"})
            if list_site_info:
                hosts_name = [info["hostname"] for info in list_site_info if info]
                for hostname in hosts_name:
                    data_host = {}
                    for direction in ["source_se", "dest_se"]:
                        time.sleep(1)
                        direction_data = self.get_data_from_to_host(hostname, direction=direction, error=error)
                        if direction_data:
                            data_host.update({direction: direction_data})
                    all_data.update({hostname: data_host})
        return all_data

    def results_to_csv(self, json_results):
        columns = ["timestamp", "timestamp_hr", "error", "se_from",
                   "se_to", "pfn_from", "pfn_to", "num_errors"]
        csv_results = []

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
                worksheet.conditional_format(column_id_error, {'type': '3_color_scale'})  # ,
                # 'min_color': "#878180",
                # 'max_color': "#D4CCCB"
                column_id_group = "M1:M{}".format(str(len(list_groups) + 1))
                worksheet.conditional_format(column_id_group, {'type': '3_color_scale'})

                print()
            writer.save()
        return csv_results


if __name__ == "__main__":
    time_class = Time(hours=48)
    fts = Transfers(time_class)
    # a = fts.analyze_site(hostname="srm-cms.gridpp.rl.ac.uk")
    dict_result = fts.analyze_site(site_name="T2_US_UCSD")  # , error="CHECKSUM")
    with open('all.json', 'w') as outfile:
        json.dump(dict_result, outfile)
    print()
    error = "User timeout over"

    with open('all.json') as outfile:
        data = json.load(outfile)
    print()
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
