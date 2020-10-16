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
        response_grouped = {}
        for error in response_clean:
            # Extract useful data
            error_log = error["data"]["reason"]
            destination = get_pfn_without_path(error["data"]["dst_url"])
            origin = get_pfn_without_path(error["data"]["src_url"])
            clean_lfn = pfn_to_lfn(error["data"]["src_url"])

            min_data = {'to': destination, 'from': origin, 'lfn': clean_lfn}
            if error_log not in list(response_grouped.keys()):
                response_grouped.update({error_log: [min_data]})
            else:
                response_grouped[error_log].append(min_data)
        return response_grouped

    def get_lfn_per_error(self, response_grouped):
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
        for error, list_sources in response_grouped.items():
            all_errors.append({error: count_repeated_elements_list([source['lfn'] for source in list_sources if source['lfn'] ])})
        return all_errors

    def get_error_per_type(self, response_grouped):
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
        return count_repeated_elements_list(list(response_grouped.keys()))


if __name__ == "__main__":
    time = Time(days=2).time_slot
    fts = Transfers(time)
    query = fts.get_query(kibana_query="data.vo:cms AND data.reason:/.*mismatch.*/")
    response = fts.get_response(query)
    response_grouped_ = fts.get_grouped_errors(response)
    lfn_errors = fts.get_lfn_per_error(response_grouped_)
    dict_num_type_errors = fts.get_error_per_type(response_grouped_)

    """
    # Write json file with the results
    json_output = json.dumps(lfn_errors)
    with open('~/Desktop/output.txt', 'w') as outfile:
        json.dump(json_output, outfile)
    """
    print(json.dumps(lfn_errors))




