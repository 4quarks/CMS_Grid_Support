# coding=utf-8

from abc import ABC
from cms_support.utils.query_utils import AbstractQueries
from cms_support.utils.constants import Constants as Cte
import json
import requests
import re
from cms_support.utils.site_utils import get_type_resource

# with open('sites_resources.json') as json_file:
#     ALL_RESOURCES = json.load(json_file)

TEST_FIELD = "metadata.path:"
ALL_RESOURCES = json.loads(requests.get("http://pcutrina.web.cern.ch/pcutrina/sites_resources.json").content)


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


def get_unique_elements_from_json(target, type_resource=""):
    if not type_resource:
        type_resource = get_type_resource(target)
    unique_resources = []
    for resource in ALL_RESOURCES:
        feature = resource[type_resource]
        if re.search(target.lower(), feature.lower()) and feature not in unique_resources:
            unique_resources.append(feature)
    return unique_resources


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


# if __name__ == "__main__":
#     a = get_unique_elements_from_json("T2")
#     print()
