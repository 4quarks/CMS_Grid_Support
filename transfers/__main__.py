# coding=utf-8
import argparse
from transfers.app import run_transfers

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='python -m transfers', description="CMS Support Tools!")
    parser.add_argument("target", help="Site or hostname to analyze", type=str)
    parser.add_argument("-hr", "--hours", help="Number of hours", type=int)
    parser.add_argument("-d", "--days", help="Number of days", type=int)
    parser.add_argument("-m", "--minutes", help="Number of minutes", type=int)
    parser.add_argument("-b", "--black", help="Blacklisted elements on the PFN separated by '|'", type=str)
    parser.add_argument("-lfn", "--write_lfns", help="Write files with all LFNS", action="store_true")
    parser.add_argument("-e", "--error", help="Keywords to search on the error log (separate by dots '.')", type=str)
    args = parser.parse_args()
    if args.error:
        args.error = args.error.replace(".", " ")
    if args.write_lfns:
        args.write_lfns = True

    run_transfers(args)
