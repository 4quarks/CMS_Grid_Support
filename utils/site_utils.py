# coding=utf-8

import re


def get_resource_from_target(target):
    site_name, hostname, flavour = "", "", ""
    if target:
        if re.search("t[0-9]_*", target.lower()):
            site_name = target
        if re.search("srm|xrootd|xrd|.*ce.*", target.lower()):
            hostname = hostname
        if re.search("([a-z]).", target.lower()):
            flavour = flavour
    return site_name, hostname, flavour










