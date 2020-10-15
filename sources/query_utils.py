import requests
import os
import json
from copy import deepcopy
from constants import Constants as Cte
import datetime


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


def pfn_to_lfn(pfn):
    lfn = ""
    if "/store" in pfn:
        lfn = "/store" + pfn.split("/store")[1]
    return lfn


def get_pfn_without_path(pfn):
    reduced_pfn = ""
    if "/store" in pfn:
        reduced_pfn = pfn.split("/store")[0]
    return reduced_pfn


def count_repeated_elements_list(list_elements):
    return {element: list_elements.count(element) for element in list_elements}





























