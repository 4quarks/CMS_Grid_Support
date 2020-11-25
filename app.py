from transfers.transfers_rucio import Transfers
from utils.query_utils import Time
from utils.transfers_utils import ExcelGenerator
from sites.site_status import SiteStatus
from utils.constants import CteSAM


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
    time_ss = Time(days=args.days, hours=args.hours, minutes=args.minutes)
    sam = SiteStatus(time_ss, target=args.target, blacklist_str=args.black)
    errors = sam.get_issues_resources()
    sam.write_excel(errors)















