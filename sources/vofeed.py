from query_utils import *

"""
data.site 	T1_ES_PIC
metadata.path 	vofeed15min

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
data.tier 	1
data.vo 	CMS
metadata.producer 	cmssst

---------------------------------------------------------------------------------
data.name 	T2_IT_Bari
metadata.path 	scap15min

data.core_cpu_intensive 	1,250
data.core_io_intensive 	25
data.core_max_used 	2,000
data.core_production 	547
data.core_usable 	1,000
data.disk_experiment_use 	1
data.disk_pledge 	500
data.disk_usable 	500
data.hs06_per_core 	10
data.hs06_pledge 	10,000
data.tape_pledge 	0
data.tape_usable 	0
data.when 	2020-Jan-17 22:12:00
data.who 	lammel
data.wlcg_federation_fraction 	1
data.wlcg_federation_name 	Austrian Tier-2 Federation

"""

TEST_FIELD = "metadata.path:"


class VOFeed(AbstractQueries, ABC):
    def __init__(self, time_slot):
        super().__init__(time_slot)
        self.index_name = "monit_prod_cmssst*"
        self.index_id = "9475"
        self.field_id = "vofeed15min"
        self.field_site_name = "data.site:"

        self.mongo = MongoDB()

    def get_resources(self, response):
        all_resources = []
        update_time_ref = ""
        for num_resp, element in enumerate(response):
            site = element['data']['site']
            update_time = element['data']['update']
            if update_time_ref and update_time_ref != update_time:
                break
            if not update_time_ref:
                update_time_ref = update_time

            # Add site name info
            for resource in element['data']['services']:
                resource.update({"site": site})
                all_resources.append(resource)
        return all_resources

    def get_site_resources(self, site_name="/.*.*/"):
        kibana_query = TEST_FIELD + self.field_id + Cte.AND + self.field_site_name + site_name
        response = self.get_direct_response(kibana_query)
        resources_site = self.get_resources(response)
        return resources_site

    def update_mongo_vofeed(self, site_name="/.*.*/"):

        all_resources = self.get_site_resources(site_name)
        self.mongo.insert_list_documents(self.mongo.vofeed, all_resources)

    def get_resource_filtered(self, flavour="", hostname="", site=""):
        query = {}
        if flavour:
            query.update({"flavour": flavour})
        if hostname:
            query.update({"hostname": hostname})
        if site:
            query.update({"site": site})
        return self.mongo.find_document(self.mongo.vofeed, query)


class SiteCapacity(AbstractQueries, ABC):
    def __init__(self, time_slot):
        super().__init__(time_slot)
        self.index_name = "monit_prod_cmssst*"
        self.index_id = "9475"
        self.field_id = "scap15min"
        self.field_site_name = "data.name:"

    def get_site_capacity(self, site_name):
        kibana_query = TEST_FIELD + self.field_id + Cte.AND + self.field_site_name + site_name
        response = self.get_direct_response(kibana_query)
        site_capacity = {}
        if response:
            site_capacity = response[0]["data"]
        return site_capacity

    def get_attribute_capacity(self, site_name, attribute):
        attr_capacity = ""
        site_capacity = self.get_site_capacity(site_name)
        if attribute in site_capacity.keys():
            attr_capacity = site_capacity[attribute]
        return attr_capacity


if __name__ == "__main__":
    time = Time(days=1).time_slot
    vofeed = VOFeed(time)
    resources = vofeed.get_resource_filtered(site="", flavour="SRM")
    print()
    # capacity = SiteCapacity(time)
    # resources = capacity.get_attribute_capacity("T1_ES_PIC", "core_usable")
    print(resources)
    "SRM, T2_CH_CERN --> hostname"
    "hostname --> SRM, T2_CH_CERN"
