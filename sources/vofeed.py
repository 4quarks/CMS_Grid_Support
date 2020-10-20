from query_utils import *


"""
data.services 	{
                  "hostname": "xrootd01-cmst1.pic.es",
                  "flavour": "XROOTD",
                  "endpoint": "xrootd01-cmst1.pic.es:1095"
                },
                {
                  "hostname": "xrootd02-cmst1.pic.es",
                  "flavour": "XROOTD",
                  "endpoint": "xrootd02-cmst1.pic.es:1095"
                },
                {
                  "hostname": "llrppce.in2p3.fr",
                  "flavour": "HTCONDOR-CE",
                  "endpoint": "llrppce.in2p3.fr:9619",
                  "production": false
                }
data.site 	T1_ES_PIC
data.tier 	1
data.vo 	CMS
metadata.path 	vofeed15min
metadata.producer 	cmssst
"""


class SAMTest(AbstractQueries, ABC):
    def __init__(self, time_slot):
        super().__init__(time_slot)
        self.index_name = "monit_prod_cmssst*"
        self.index_id = "9475"


if __name__ == "__main__":
    time = Time(days=1).time_slot
    sam = SAMTest(time)

    kibana_query = "metadata.path:vofeed15min"
    query_general = sam.get_query(kibana_query=kibana_query)

    response = sam.get_response(query_general)
    for element in response:
        site_name = element['data']['site']
        for service in element['data']['services']:
            print('SERVICE: ', service)


























