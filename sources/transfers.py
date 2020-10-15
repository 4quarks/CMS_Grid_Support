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






"""

# reason = "data.reason:/.*Communication error on send.*/"
# reason = "data.reason:\"DESTINATION [17] Destination file exists and overwrite is not enabled\""
# reason = "data.reason:/.*CHECKSUM MISMATCH.*/"
reason = "data.reason:/.*DESTINATION.*/"
drop_hostname = True
# reason = "NOT data.reason:\"DESTINATION [17] Destination file exists and overwrite is not enabled\""
# drop_hostname = False

query = "data.dst_url:/.*storm-fe-cms.cr.cnaf.infn.it.*/ AND data.file_state:FAILED AND " + reason


# query="data.vo:cms AND NOT data.source_se:/.*se.cis.gov.pl.*/ AND data.dest_se:/.*storm-fe-cms.cr.cnaf.infn.it.*/ AND data.file_state:FAILED AND data.reason:/.*CGSI-gSOAP.*/"


class Status:
    error = "FAILED"
    active = "ACTIVE"
    ready = "READY"
    submitted = "SUBMITTED"
    finished = "FINISHED"


class TransferAttributes:
    attr_destination = "data.dst_url:"
    attr_source = "data.src_url:"
    attr_status = "data.file_state:"
    attr_reason = "data.reason:"


class Time:
    def __init__(self, days=0, hours=12, minutes=0, seconds=0):
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.time_slot = self.translate_time()

    def translate_time(self):
        now_datetime = datetime.datetime.now()
        previous_datetime = now_datetime - datetime.timedelta(days=self.days, hours=self.hours, minutes=self.minutes,
                                                              seconds=self.seconds)
        max_time = round(datetime.datetime.timestamp(now_datetime)) * 1000
        min_time = round(datetime.datetime.timestamp(previous_datetime)) * 1000
        return [min_time, max_time]


class Transfers(TransferAttributes, Status):
    def __init__(self, time_slot):
        super().__init__()
        self.index = {"name": "monit_prod_fts_raw_*", "id": "9233"}
        self.errors_dict = {}
        self.time_slot = time_slot

    def get_query(self, dst_url="", src_url="", status="", reason=""):
        raw_query = "data.vo:cms"
        if dst_url:
            raw_query += Cte.AND + self.attr_destination + Cte.INCLUDED.format(dst_url)
        if src_url:
            raw_query += Cte.AND + self.attr_source +Cte.INCLUDED.format( src_url)
        if status:
            raw_query += Cte.AND + self.attr_status + status
        if reason:
            raw_query += Cte.AND + self.attr_reason + Cte.INCLUDED.format(reason)

        clean_str_query = get_str_lucene_query(self.index['name'],
                                               self.time_slot[0], self.time_slot[1], raw_query)
        return clean_str_query

    def get_response(self, clean_query):
        raw_response = json.loads(get_data_grafana(self.index['id'], clean_query).text.encode('utf8'))
        return get_clean_results(raw_response)

    def get_grouped_results(self, response_clean):
        response_grouped = {}
        for error in response_clean:
            # Extract useful data
            error_log = error["data"]["reason"]
            destination = get_pfn_without_path(error["data"]["dst_url"])
            origin = get_pfn_without_path(error["data"]["src_url"])
            clean_lfn = pfn_to_lfn(error["data"]["src_url"])

            min_data = {'dst': destination, 'src': origin, 'lfn': clean_lfn}
            if error_log not in list(response_grouped.keys()):
                response_grouped.update({error_log: [min_data]})
            else:
                response_grouped[error_log].append(min_data)
        return response_grouped

    def get_lfn_per_error(self, response_grouped):
        """

        :param response_grouped:
        :return:
        """
        all_errors = []
        for error, list_sources in response_grouped.items():
            all_errors.append({error: count_repeated_elements_list([source['lfn'] for source in list_sources])})
        return all_errors

    def get_error_per_type(self, response_grouped):
        return count_repeated_elements_list(list(response_grouped.keys()))

    def get_unique_errors(self, response_grouped):
        return [ele.strip() for ele in list(dict.fromkeys(list(response_grouped.keys()))) if ele]


if __name__ == "__main__":
    time_slot = Time(days=2).time_slot
    fts = Transfers(time_slot)
    query = fts.get_query(dst_url="storm-fe-cms.cr.cnaf.infn.it", status=fts.error,
                          reason="Destination file exists and overwrite is not enabled")
    response = fts.get_response(query)
    response_grouped_ = fts.get_grouped_results(response)
    lfn_errors = fts.get_lfn_per_error(response_grouped_)
    print(json.dumps(lfn_errors))