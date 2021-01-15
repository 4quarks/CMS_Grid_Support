# coding=utf-8

from cms_support.sites.vofeed import VOFeed
from abc import ABC
from cms_support.utils.query_utils import AbstractQueries, Time


class Jobs(AbstractQueries, ABC):
    def __init__(self, time_slot):
        super().__init__(time_slot)
        self.index_name = "monit_prod_condor_raw_metric*"
        self.index_id = "9668"

    def detect_HC_outage(self, site_name="/.*.*/"):
        kibana_query = "data.DESIRED_Sites:{} AND data.Type:test AND data.Status:Running".format(site_name)
        response1 = self.get_direct_response(kibana_query=kibana_query)
        sites_list = []
        if site_name == "/.*.*/":
            vofeed = VOFeed("")
            sites = vofeed.get_list(field="site")
            for site in sites:
                kibana_query = "data.DESIRED_Sites:{} AND data.Type:test AND data.Status:Running".format(site)
                response = self.get_direct_response(kibana_query=kibana_query)

                if not response:
                    sites_list.append(site)
        print(sites_list)


# if __name__ == "__main__":
#     time = Time(hours=6)
#     jobs = Jobs(time)
#     kibana_query = "data.DESIRED_Sites:T2_US_MIT AND data.Type:test"
#     jobs.detect_HC_outage()


    # print(response)



































































