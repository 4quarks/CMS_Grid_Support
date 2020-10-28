from query_utils import *
import spacy

NLP = spacy.load("en_core_web_lg")
THRESHOLD = 0.99

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
"""


class Transfers(AbstractQueries, ABC):
    def __init__(self, time_class):
        super().__init__(time_class)
        self.index_name = "monit_prod_fts_raw_*"
        self.index_id = "9233"
        self.mongo = MongoDB()

    def create_str_link(self, origin, destination):
        return str(origin) + '__' + destination

    def value_in_list(self, value, list_values, strict_comparison=False):
        in_list = False
        similar_value = value
        if strict_comparison:
            if value in list_values:
                in_list = True
        else:
            doc = NLP(value)
            for a in list_values:
                if a:
                    doc1 = NLP(a)  # Finding the similarity
                    score = doc.similarity(doc1)
                    if score > THRESHOLD:
                        in_list = True
                        similar_value = a
                        break
        return in_list, similar_value

    def group_data(self, grouped_all, min_dict, group_ref, existent_element):
        # value_in_list, similar_value = self.value_in_list(group_ref,  list(grouped_all.keys()), strict_comparison)
        if not existent_element:
            grouped_all.update({group_ref: [min_dict]})
        else:
            matched_element = False
            for idx, element in enumerate(grouped_all[group_ref]):
                # SAME ERROR & SAME PFN --> SAME ISSUE
                if element['pfn_from'] == min_dict['pfn_from'] and element['pfn_to'] == min_dict['pfn_to']:
                    grouped_all[group_ref][idx]['num'] = grouped_all[group_ref][idx]['num'] + 1
                    matched_element = True
                    break

            if not matched_element:
                grouped_all[group_ref].append(min_dict)
        return grouped_all

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

                min_data = {'pfn_from': pfn_src, 'pfn_to': pfn_dst, 'num': 1, 'error': error_log}

                # Append Error data
                if error_log:
                    min_error = deepcopy(min_data)
                    min_error.update({'se_from': source, 'se_to': destination})
                    error_in_list, similar_error = self.value_in_list(error_log, list(grouped_by_error.keys()))
                    grouped_by_error = self.group_data(grouped_by_error, min_error, similar_error, error_in_list)

                    if similar_error != error_log:
                        if similar_error not in errors_grouped_nlp.keys():
                            errors_grouped_nlp.update({similar_error: []})
                        errors_grouped_nlp[similar_error].append(error_log)
                    # Append site data
                    min_sites = deepcopy(min_data)
                    link = self.create_str_link(source, destination)
                    link_in_list, similar_link = self.value_in_list(link, list(grouped_by_site.keys()),
                                                                    strict_comparison=True)
                    grouped_by_site = self.group_data(grouped_by_site, min_sites, similar_link, link_in_list)

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
        kibana_query = "data.vo:cms AND data.file_state:FAILED AND data.{}:/.*{}.*/ ".format(direction, hostname)
        if error:
            kibana_query += " AND data.reason:/.*\"{}\".*/".format(error)
        response = self.get_direct_response(kibana_query=kibana_query, max_results=2000)
        grouped_by_error, grouped_by_site = self.get_grouped_errors(response)
        lfn_errors, _ = self.get_lfn_per_error(grouped_by_error)
        host_data = {}
        if grouped_by_error and grouped_by_site:
            host_data = {"time_range_UTC": self.time_class.time_slot_hr,
                         "grouped_by_error": grouped_by_error,
                         "grouped_by_site": grouped_by_site,
                         "list_lfn": lfn_errors}
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
                        direction_data = self.get_data_from_to_host(hostname, direction=direction, error=error)
                        if direction_data:
                            data_host.update({direction: direction_data})
                    all_data.update({hostname: data_host})
        return all_data


if __name__ == "__main__":
    time_class = Time(hours=2)
    fts = Transfers(time_class)
    # a = fts.analyze_site(hostname="srm-cms.gridpp.rl.ac.uk")
    a = fts.analyze_site(site_name="T1_US_FNAL", error="CHECKSUM")
    with open('all.json', 'w') as outfile:
        json.dump(a, outfile)
    print()
    error = "User timeout over"
    # kibana_query = "data.vo:cms AND source_se:/.*storm.ifca.es.*/ AND data.reason:/.*\"{}\".*/".format(error)
    # kibana_query = "data.vo:cms  AND data.source_se:/.*gftp.t2.ucsd.edu.*/ AND data.file_state:FAILED " \
    #                "AND data.reason:/.*CHECKSUM.*|.*No such file.*/"

    # endpoint = "srm-cms.gridpp.rl.ac.uk"
    # # reason = "AND data.reason:/.*\"User timeout over\".*/"
    # reason = ""
    # kibana_query = "data.vo:cms AND data.file_state:FAILED AND " \
    #                "(data.source_se:/.*{}.*/ OR data.dest_se:/.*{}.*/) {}".format(endpoint, endpoint, reason)
    # # kibana_query = "data.vo:cms  AND data.dest_se:/.*eoscmsftp.cern.ch.*/ AND data.file_state:FAILED"
