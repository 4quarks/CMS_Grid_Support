from query_utils import *

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
    def __init__(self, time_slot):
        super().__init__(time_slot)
        self.index_name = "monit_prod_fts_raw_*"
        self.index_id = "9233"

    def create_str_link(self, origin, destination):
        return str(origin) + '__' + destination

    def group_data(self, grouped_all, min_dict, group_ref):
        # group_ref = group_ref[:20]
        if group_ref not in list(grouped_all.keys()):
            grouped_all.update({group_ref: [min_dict]})
        else:
            for element in grouped_all[group_ref]:
                if element['pfn_from'] == min_dict['pfn_from'] and element['pfn_to'] == min_dict['pfn_to']:
                    element['num'] = + 1

                else:
                    grouped_all[group_ref].append(min_dict)
        return grouped_all

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
        for error in response_clean:
            # Extract useful data
            if "reason" in error["data"].keys():
                error_log = error["data"]["reason"]
                # Source
                pfn_src = error["data"]["src_url"]
                source = error["data"]["source_se"].split('://')[1]
                # Destination
                pfn_dst = error["data"]["dst_url"]
                destination = error["data"]["dest_se"].split('://')[1]

                min_data = {'pfn_from': pfn_src, 'pfn_to': pfn_dst, 'num': 1}

                # Append Error data
                min_error = deepcopy(min_data)
                min_error.update({'se_from': source, 'se_to': destination})
                grouped_by_error = self.group_data(grouped_by_error, min_error, error_log)

                # Append site data
                min_sites = deepcopy(min_data)
                min_sites.update({'error': error_log})
                link = self.create_str_link(source, destination)
                grouped_by_site = self.group_data(grouped_by_site, min_sites, link)

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
        for error, list_min_data in grouped_by_error.items():
            all_errors.append({error: [{min_data['lfn']: min_data['num']} for min_data in list_min_data]})
        return all_errors

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


if __name__ == "__main__":
    time = Time(hours=6).time_slot
    fts = Transfers(time)
    query = fts.get_query(kibana_query="data.vo:cms AND data.source_se:/.*dcache-se-cms.desy.de.*/ "
                                       "AND data.file_state:FAILED AND NOT data.dest_se:\"srm://se3.itep.ru\"")
    response = fts.get_response(query)
    grouped_by_error_, grouped_by_site_ = fts.get_grouped_errors(response)

    print('grouped_by_error_')
    json_errors = json.dumps(grouped_by_error_)
    print(json_errors, '\n')
    with open('errors_grouped_CNAF.json', 'w') as outfile:
        json.dump(grouped_by_error_, outfile)

    print('grouped_by_site_')
    json_sites = json.dumps(grouped_by_site_)
    print(json_sites, '\n')
    with open('sites_grouped_CNAF.json', 'w') as outfile:
        json.dump(grouped_by_site_, outfile)

    # print('lfn_errors')
    # lfn_errors = fts.get_lfn_per_error(grouped_by_error_)
    # print(json.dumps(lfn_errors), '\n')

    # print('dict_num_type_errors')
    # dict_num_type_errors = fts.get_error_per_type(grouped_by_error_)
    # print(json.dumps(dict_num_type_errors), '\n')



