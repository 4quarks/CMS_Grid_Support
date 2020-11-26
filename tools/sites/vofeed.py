# coding=utf-8

from abc import ABC
from tools.utils.query_utils import AbstractQueries
from tools.utils.constants import Constants as Cte
import json
import requests
import re

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
ALL_RESOURCES = json.loads(requests.get("https://pcutrina.web.cern.ch/pcutrina/sites_resources.json").content)


def get_resources_from_json(site="", hostname="", flavour=""):
    list_resources = []
    dict_components = {"site": site, "hostname": hostname, "flavour": flavour}
    for resource in ALL_RESOURCES:
        match_resource = True
        for key_comp, val_comp in dict_components.items():
            if val_comp:
                if not re.search(val_comp, resource[key_comp]):
                    match_resource = False
        if match_resource:
            list_resources.append(resource)
    return list_resources


class VOFeed(AbstractQueries, ABC):
    def __init__(self, time):
        super().__init__(time)
        self.index_name = "monit_prod_cmssst*"
        self.index_id = "9475"
        self.field_id = "vofeed15min"
        self.field_site_name = "data.site:"

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

    def get_site_resources(self, site_name=""):
        kibana_query = TEST_FIELD + self.field_id + Cte.AND + self.field_site_name + "/.*.*/".format(site_name)
        response = self.get_direct_response(kibana_query)
        resources_site = self.get_resources(response)
        return resources_site


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
    pass
