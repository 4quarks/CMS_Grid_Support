# coding=utf-8

from cms_support.sites.vofeed import get_resources_from_json
from cms_support.sites.sam3 import SAMTest
from cms_support.utils.constants import CteSAM as CteSAM
from cms_support.utils.query_utils import timestamp_to_human_utc
from cms_support.utils.nlp_utils import group_data
from copy import deepcopy
from cms_support.utils.site_utils import get_resource_from_target, AbstractCMSSST


class SiteStatus(AbstractCMSSST):
    def __init__(self, time_class, target="", flavour="", blacklist_regex=""):
        super().__init__(time_class, CteSAM.REF_SAM_METRIC, blacklist_regex=blacklist_regex)

        self.flavour = self.get_ref_flavour(flavour)

        self.site_name, self.hostname, _ = get_resource_from_target(target)

        self.site_resources = get_resources_from_json(site=self.site_name, hostname=self.hostname, flavour=self.flavour)

        self.sam3 = SAMTest(time_class)

    @staticmethod
    def get_issues(response_all):
        list_errors, all_status = [], []
        num_status_row, num_errors = 0, 0
        for num_test, test in enumerate(response_all):
            test_data = test[CteSAM.REF_DATA]
            status_test = test_data[CteSAM.REF_STATUS]
            all_status.append(status_test)
            if status_test != "ok":
                num_errors += 1
                # DETAILED ERROR EVALUATION
                if CteSAM.REF_LOG_CMSSST in test_data.keys():
                    details = test_data[CteSAM.REF_LOG_CMSSST]
                    if details:
                        metrics = [elem for elem in details.split(",") if elem]
                        for elem in metrics:
                            metric_error = elem.split("(")[0].strip()
                            if metric_error not in list_errors and status_test != "ok":
                                list_errors.append(elem.split("(")[0].strip())
            if all_status and all_status[num_test-1] == status_test:
                num_status_row += 1
        most_frequent_status = max(set(all_status), key=all_status.count)
        return most_frequent_status, list_errors, num_status_row, num_errors

    def get_issues_resources(self):
        rows = []

        for resource in self.site_resources:
            hostname = resource[CteSAM.REF_HOST]
            flavour = self.get_ref_flavour(resource[CteSAM.REF_FLAVOUR])
            site = resource[CteSAM.REF_SITE]

            is_test_endpoint = "production" in resource.keys()
            is_blacklisted = self.is_blacklisted(site)

            if not is_test_endpoint and not is_blacklisted:
                dict_attr = {"type": flavour, "name":hostname}
                kibana_query_all = self.get_kibana_query(dict_attr=dict_attr)
                response_kibana_cmssst = self.get_direct_response(kibana_query=kibana_query_all)
                if response_kibana_cmssst:
                    status, list_errors, num_errors_row, num_not_ok_tests = self.get_issues(response_kibana_cmssst)
                    if status != "ok" and list_errors:
                        if status != "unknown":
                            for metric_with_error in list_errors:
                                response_kibana_sam3 = self.sam3.get_details_test(hostname=hostname,
                                                                                  metric_name=metric_with_error)
                                ############ GROUP DATA BY ERRORS ############
                                grouped_by_error = {}
                                ############ ITERATE OVER ALL ERRORS ############
                                for error in response_kibana_sam3:
                                    # Extract useful data
                                    if CteSAM.REF_LOG in error[CteSAM.REF_DATA].keys():
                                        ############ GROUP THE ERROR ############
                                        error_data = deepcopy(error[CteSAM.REF_DATA])
                                        timestamp_hr = timestamp_to_human_utc(error_data[CteSAM.REF_TIMESTAMP])
                                        error_data.update({CteSAM.REF_TIMESTAMP_HR: timestamp_hr})

                                        grouped_by_error = group_data(grouped_by_error, error_data, ['metric_name'],
                                                                      CteSAM)
                                for error_grouped, value_error in grouped_by_error.items():
                                    for single_error in value_error:
                                        rows.append(
                                            [single_error[CteSAM.REF_TIMESTAMP_HR], site, hostname, flavour, status,
                                             num_not_ok_tests, num_errors_row, metric_with_error, error_grouped,
                                             single_error[CteSAM.REF_NUM_ERRORS]])
                        else:
                            rows.append(
                                ["time", site, hostname, flavour, status, num_not_ok_tests,
                                 num_errors_row,
                                 str(list_errors), None, None])
        return rows


















