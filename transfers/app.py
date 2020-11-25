from transfers.transfers_rucio import Transfers
from utils.query_utils import Time
from utils.transfers_utils import ExcelGenerator


def run_transfers(args):
    time_class = Time(days=args.days, hours=args.hours, minutes=args.minutes)
    fts = Transfers(time_class)
    dict_result = fts.analyze_site(target=args.target, filter_error_kibana=args.error, str_blacklist=args.black)
    generator = ExcelGenerator(dict_result, args.target)
    generator.results_to_csv(write_lfns=args.write_lfns)

