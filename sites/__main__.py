import argparse
from app import run_site_status

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='python -m transfers', description="CMS Support Tools!")
    parser.add_argument("target", help="Site or hostname to analyze", type=str)
    parser.add_argument("-hr", "--hours", help="Number of hours", type=int)
    parser.add_argument("-d", "--days", help="Number of days", type=int)
    parser.add_argument("-m", "--minutes", help="Number of minutes", type=int)
    parser.add_argument("-b", "--black", help="Blacklisted elements on the PFN separated by '|'", type=str)

    args = parser.parse_args()

    run_site_status(args)
