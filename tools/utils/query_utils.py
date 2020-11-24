import requests
import os
import json
from copy import deepcopy
import datetime
from abc import ABC


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


def unique_elem_list(list_repeated_elem):
    return list(dict.fromkeys(list_repeated_elem))


def timestamp_to_human_utc(timestamp):
    human_time = ""
    if "int" in str(type(timestamp)):
        human_time = datetime.datetime.utcfromtimestamp(timestamp/1000).strftime('%d-%m-%Y %H:%M')
    return human_time


def get_str_lucene_query(index_es, min_time, max_time, query, max_results):
    BASIC_QUERY["size"] = max_results
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


class ClassFromDict:
    def __init__(self, **entries):
        self.__dict__.update(entries)

    def to_dict(self):
        return self.__dict__


def get_lfn_and_short_pfn(raw_pfn):
    lfn, reduced_pfn = "", ""
    if "/store" in raw_pfn:
        lfn = "/store" + raw_pfn.split("/store")[1]
        reduced_pfn = raw_pfn.split("/store")[0]
    return lfn, reduced_pfn


def count_repeated_elements_list(list_elements):
    return {element: list_elements.count(element) for element in list_elements}


class Time:
    def __init__(self, days=0, hours=0, minutes=0, seconds=0):
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.time_slot = self.translate_time()
        self.time_slot_hr = [self.datetime_to_human(self.time_slot[0]), self.datetime_to_human(self.time_slot[1])]

    def translate_time(self):
        now_datetime = datetime.datetime.utcnow()
        previous_datetime = now_datetime - datetime.timedelta(days=self.days, hours=self.hours, minutes=self.minutes,
                                                              seconds=self.seconds)
        max_time = round(datetime.datetime.timestamp(now_datetime)) * 1000
        min_time = round(datetime.datetime.timestamp(previous_datetime)) * 1000
        return [min_time, max_time]

    def datetime_to_human(self, timestamp):
        return datetime.datetime.utcfromtimestamp(int(timestamp/1000)).strftime('%d-%m-%Y %H:%M')


class AbstractQueries(ABC):
    def __init__(self, time_class):
        self.index_name = ""
        self.index_id = ""
        self.time_class = time_class

    def get_direct_response(self, kibana_query, max_results=500):
        return self.get_response(self.get_query(kibana_query, max_results))

    def get_query(self, kibana_query="", max_results=500):
        """

        :param kibana_query:
        :return: dicts (same as Postman) necessary to query via request
        e.g.
        {"search_type":"query_then_fetch","ignore_unavailable":true,"index":"monit_prod_fts_raw_*"}
        {"size": 500, "query": {"bool": {"filter": [{"range": {"metadata.timestamp": {"gte": 1602634230000, ....
        """
        clean_str_query = ""
        if max_results > 10000:
            raise Exception("max_results has to be <= 10.000")
        if kibana_query:
            raw_query = kibana_query
            clean_str_query = get_str_lucene_query(self.index_name,
                                                   self.time_class.time_slot[0], self.time_class.time_slot[1],
                                                   raw_query, max_results)
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
        url = "https://monit-grafana.cern.ch/api/datasources/proxy/" + self.index_id + "/_msearch"
        headers = {'Content-Type': 'application/json', 'Authorization': os.environ["GRAFANA_KEY"]}
        raw_response = requests.request("POST", url, headers=headers, data=clean_query).text.encode('utf8')
        if "Unauthorized" in str(raw_response):
            raise Exception("Invalid Grafana Key, remember to export GRAFANA_KEY='Bearer FNJZ0gyS...'")
        json_response = json.loads(raw_response)
        response_clean = []
        if json_response:
            response_clean = [element["_source"] for element in json_response['responses'][0]['hits']['hits']]
        return response_clean


class AbstractNLP:
    def __init__(self, raw_text):
        self.raw_text = raw_text
        self.clean_text = self.preprocess_string_nlp(raw_text)

    def preprocess_string_nlp(self, text):
        return text.lower().strip()

