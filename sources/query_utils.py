import requests
import os
import json
from copy import deepcopy
from constants import Constants as Cte
import datetime
from abc import ABC, abstractmethod


def get_data_grafana(url_idx, query):
    url = "https://monit-grafana.cern.ch/api/datasources/proxy/" + url_idx + "/_msearch"
    headers = {'Content-Type': 'application/json', 'Authorization': os.environ["GRAFANA_KEY"]}
    response = requests.request("POST", url, headers=headers, data=query)
    return response


FIRST_QUERY = {
    "search_type": "query_then_fetch",
    "ignore_unavailable": True,
    "index": []
}

BASIC_QUERY = {
    "size": 500,
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        "metadata.timestamp": {
                            "gte": None,
                            "lte": None,
                            "format": "epoch_millis"
                        }
                    }
                },
                {
                    "query_string": {
                        "analyze_wildcard": True,
                        "query": ""
                    }
                }
            ]
        }
    }
}


def get_str_lucene_query(index_es, min_time, max_time, query):
    first_query = deepcopy(FIRST_QUERY)
    basic_query = deepcopy(BASIC_QUERY)

    first_query["index"] = index_es

    basic_query["query"]["bool"]["filter"][0]["range"]["metadata.timestamp"]["gte"] = min_time
    basic_query["query"]["bool"]["filter"][0]["range"]["metadata.timestamp"]["lte"] = max_time
    basic_query["query"]["bool"]["filter"][1]["query_string"]["query"] = query

    first_query_str = str(json.dumps(first_query)).replace(' ', '')

    basic_query_str = str(json.dumps(basic_query))
    clean_str_query = basic_query_str.replace(query.replace(' ', ''), query)

    final_query = first_query_str + " \n" + clean_str_query + " \n"

    return final_query


def get_clean_results(raw_results):
    return [element["_source"] for element in raw_results['responses'][0]['hits']['hits']]


def get_lfn_and_short_pfn(raw_pfn):
    lfn, reduced_pfn = "", ""
    if "/store" in raw_pfn:
        lfn = "/store" + raw_pfn.split("/store")[1]
        reduced_pfn = raw_pfn.split("/store")[0]
    return lfn, reduced_pfn


def count_repeated_elements_list(list_elements):
    return {element: list_elements.count(element) for element in list_elements}


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


class AbstractQueries(ABC):
    def __init__(self, time_slot):
        self.index_name = ""
        self.index_id = ""
        self.time_slot = time_slot

    def get_query(self, kibana_query=""):
        """

        :param kibana_query:
        :return: dicts (same as Postman) necessary to query via request
        e.g.
        {"search_type":"query_then_fetch","ignore_unavailable":true,"index":"monit_prod_fts_raw_*"}
        {"size": 500, "query": {"bool": {"filter": [{"range": {"metadata.timestamp": {"gte": 1602634230000, ....
        """
        clean_str_query = ""
        if kibana_query:
            raw_query = kibana_query
            clean_str_query = get_str_lucene_query(self.index_name, self.time_slot[0], self.time_slot[1], raw_query)
        return clean_str_query

    def get_response(self, clean_query):
        """
        Get clean response (without ["query"]["bool"]["filter"][0]["range"]....)
        :param clean_query:
        :return: list of dicts with all the response (same as Kibana)
        e.g.
        [
            {'data':
                {
                    'src_url': 'gsiftp://transfer.ultralight.org:28...',
                    'dst_url': 'gsiftp://se.cis.gov.pl:2811/,
                    ...
        """
        raw_response = json.loads(get_data_grafana(self.index_id, clean_query).text.encode('utf8'))
        response_clean = get_clean_results(raw_response)
        return response_clean


