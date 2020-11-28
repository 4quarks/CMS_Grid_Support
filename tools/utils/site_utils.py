# coding=utf-8

import re
import pandas as pd
import time
from tools.utils.constants import logging


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
        logging.error("No results found. The Excel file was not created.")




