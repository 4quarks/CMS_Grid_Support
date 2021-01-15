# coding=utf-8

import argparse
from cms_support.app import run_transfers, run_site_status, run_site_readiness
from cms_support.utils.constants import Constants


def main():
    parser = argparse.ArgumentParser(prog='python -m '+Constants.PACKAGE_NAME, description="CMS Support Tools!")
    subparsers = parser.add_subparsers(dest='cmd', help='sub-command help', title="cmd")
    subparsers.required = True

    ################### TRANSFERS ###################
    parser_a1 = subparsers.add_parser('transfers', help='Generate an Excel file reporting all the transfers errors')
    parser_a1.add_argument("target", help="Site or hostname to analyze", type=str)
    parser_a1.add_argument("-hr", "--hours", help="Number of hours", type=int)
    parser_a1.add_argument("-d", "--days", help="Number of days", type=int)
    parser_a1.add_argument("-m", "--minutes", help="Number of minutes", type=int)
    parser_a1.add_argument("-b", "--black", help="Blacklisted elements on the PFN separated by '|'", type=str)
    parser_a1.add_argument("-lfn", "--write_lfns", help="Write files with all LFNS", action="store_true")
    parser_a1.add_argument("-e", "--error", help="Keywords to search on the error log (separate by dots '.')", type=str)

    ################### SITE SUPPORT ###################
    parser_b = subparsers.add_parser('status', help='Generate an Excel file reporting all the failed SAM tests')
    parser_b.add_argument("target", help="Site or hostname to analyze", type=str)
    parser_b.add_argument("-f", "--flavour", help="Flavour of the target resource (XRD, CE, SRM)", type=str)
    parser_b.add_argument("-hr", "--hours", help="Start analysis # hours ago", type=int)
    parser_b.add_argument("-d", "--days", help="Start analysis # days ago", type=int)
    parser_b.add_argument("-m", "--minutes", help="Start analysis # minutes ago", type=int)
    parser_b.add_argument("-b", "--black", help="Blacklisted sites or hosts separated by '|'", type=str)

    ################### SITE SUPPORT ###################
    parser_b = subparsers.add_parser('readiness', help='Generate an Excel file reporting all the not enabled sites')
    parser_b.add_argument("target", help="Site or hostname to analyze", type=str)
    parser_b.add_argument("-me", "--metric", help="Metric of interest (prod, crab or life) separated by '|'", type=str)
    parser_b.add_argument("-b", "--black", help="Blacklisted sites separated by '|'", type=str)

    args = parser.parse_args()

    if args.cmd == 'transfers':
        run_transfers(args)
    elif args.cmd == 'status':
        run_site_status(args)
    elif args.cmd == 'readiness':
        run_site_readiness(args)


if __name__ == "__main__":
    main()



















