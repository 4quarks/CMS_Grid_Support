# coding=utf-8

import re
import pandas as pd
import time
from cms_support.utils.constants import logging
from abc import ABC
from cms_support.utils.query_utils import AbstractQueries
from cms_support.utils.constants import CteSAM as CteSAM


def get_type_resource(target):
    type_resource = ""
    if target:
        if re.search("t[0-9]_*", target.lower()):
            type_resource = "site"
        if re.search("[a-z]\.", target.lower()):
            type_resource = "hostname"
        if re.search("srm|xrootd|xrd|.*ce.*", target.lower()):
            type_resource = "flavour"
    return type_resource


def get_resource_from_target(target):
    site, host, flavour = "", "", ""
    type_resource = get_type_resource(target)
    if type_resource == "site":
        site = target
    elif type_resource == "hostname":
        host = target
    elif type_resource == "flavour":
        host = flavour
    return site, host, flavour


def write_excel(rows, columns, name_ref):
    if columns and rows:
        timestamp_now = round(time.time())
        file_name = "{}_{}".format(timestamp_now, name_ref)
        writer = pd.ExcelWriter(file_name + ".xlsx", engine='xlsxwriter')
        df_group = pd.DataFrame(rows, columns=columns)
        df_group.to_excel(writer, index=False)
        writer.save()
        logging.info("Excel file created successfully")
    else:
        logging.info("No results found. The Excel file was not created.")


class AbstractCMSSST(AbstractQueries, ABC):
    def __init__(self, time_class, metric, str_freq="15min", target="", blacklist_regex=""):
        super().__init__(time_class, target, blacklist_regex)
        self.index_name = CteSAM.INDEX_ES
        self.index_id = CteSAM.INDEX_ES_ID
        self.metric = metric
        self.str_freq = str_freq
        self.basic_kibana_query = "metadata.path:" + self.metric + self.str_freq



    @staticmethod
    def get_ref_flavour(flavour):
        ref_flavour = flavour
        if "CE" in flavour:
            ref_flavour = "CE"
        if "XROOTD" in flavour:
            ref_flavour = "XRD"
        return ref_flavour


