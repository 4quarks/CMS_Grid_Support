from tools.transfers.transfers_rucio import Transfers
from tools.utils.query_utils import Time
from tools.utils.transfers_utils import ExcelGenerator
from tools.sites.site_status import SiteStatus, SiteReadiness
from tools.utils.site_utils import write_excel
from tools.utils.constants import CteSAM


def run_transfers(args):
    if not args.days and not args.hours and not args.minutes:
        args.days = 1
    time_class = Time(days=args.days, hours=args.hours, minutes=args.minutes)
    fts = Transfers(time_class)
    dict_result = fts.analyze_site(target=args.target, filter_error_kibana=args.error, str_blacklist=args.black)
    generator = ExcelGenerator(dict_result, args.target)
    generator.results_to_csv(write_lfns=args.write_lfns)


def run_site_status(args):
    if not args.days and not args.hours and not args.minutes:
        args.hours = CteSAM.HOURS_RANGE
    if not args.black:
        args.black = ""
    if not args.flavour:
        args.flavour = ""
    time_ss = Time(days=args.days, hours=args.hours, minutes=args.minutes)
    sam = SiteStatus(time_ss, target=args.target, flavour=args.flavour, blacklist_regex=args.black)
    errors = sam.get_issues_resources()

    columns = [CteSAM.REF_TIMESTAMP_HR, CteSAM.REF_SITE, CteSAM.REF_HOST, CteSAM.REF_FLAVOUR, CteSAM.REF_STATUS,
               'num_row_failures', 'num_failed_tests', 'failed_test', CteSAM.REF_LOG, CteSAM.REF_NUM_ERRORS]

    name_ref = "{}_{}".format(args.target, "SiteStatus")
    write_excel(errors, columns=columns, name_ref=name_ref)


def run_site_readiness(args):
    if not args.days and not args.hours and not args.minutes:
        args.hours = CteSAM.HOURS_RANGE
    if not args.black:
        args.black = ""
    time_ss = Time(days=args.days, hours=args.hours, minutes=args.minutes)
    sam = SiteReadiness(time_ss, site_regex=args.target, blacklist_regex=args.black)
    rows = sam.get_not_enabled_sites(metric=args.metric)
    columns = ["name"] + CteSAM.REF_METRICS_SR + ["detail"]
    write_excel(rows, columns=columns, name_ref=args.target + "_SiteReadiness")















