import argparse
import time
import random
import logging
import datetime
import threading
import os
import re
from rucio.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.rse import rsemanager
from rucio.common.exception import (
    InvalidRSEExpression,
    NoFilesUploaded,
    DataIdentifierNotFound,
    RSEBlacklisted,
    DestinationNotAccessible,
    ServiceUnavailable,
    ReplicaNotFound,
)


logger = logging.getLogger(__name__)
ALLOWED_FILESIZES = {
    # motivation: one file every 6h = 100kbps avg. rate
    "270MB": 270000000,
}
LOADTEST_DATASET_FMT = "/LoadTestSource/{rse}/TEST#{filesize}"
LOADTEST_LFNDIR_FMT = "/store/test/loadtest/source/{rse}/"
LOADTEST_LFNBASE_FMT = "urandom.{filesize}.file{filenumber:04d}"
FILENUMBER_SEARCH = "/store/test/loadtest/source/{rse}/urandom.{filesize}.file*"
FILENUMBER_RE = re.compile(r".*\.file(\d+)$")
DEFAULT_RULE_COMMENT = "rate:100kbps"
TARGET_CYCLE_TIME = 60 * 5
ACTIVE = True


def run(source_rse_expression, dest_rse_expression, account, activity, filesize):
    logger.info('RUNNING')
    if filesize not in ALLOWED_FILESIZES:
        raise ValueError("File size {filesize} not allowed".format(filesize=filesize))

    client = Client()

    cycle_start = datetime.datetime.utcnow()
    source_rses = [item["rse"] for item in client.list_rses(source_rse_expression)]
    logger.info('Source rses: ' + str(source_rses) + '\n' + str(client.list_rses(source_rse_expression)))
    dest_rses = [item["rse"] for item in client.list_rses(dest_rse_expression)]
    logger.info('Dest rses: ' + str(dest_rses) + str(client.list_rses(dest_rse_expression)))
    for source_rse in source_rses:
        dataset = LOADTEST_DATASET_FMT.format(rse=source_rse, filesize=filesize)
        try:
            source_files = list(client.list_files("cms", dataset))
        except DataIdentifierNotFound:
            logger.info(
                "RSE {source_rse} has no source files, will create one".format(
                    source_rse=source_rse
                )
            )
            source_files = []


if __name__ == "__main__":
    # ./loadtest.py -v   --source_rse_expression T2_CH_CERN --dest_rse_expression T2_IN_TIFR

    parser = argparse.ArgumentParser(
        description="Create periodic transfers between RSEs to test links"
    )
    parser.add_argument(
        "--source_rse_expression", type=str, help="Source RSEs to test links from"
    )
    parser.add_argument(
        "--dest_rse_expression", type=str, help="Destination RSEs to test links to"
    )
    parser.add_argument(
        "--account",
        type=str,
        default="pcutrina",
        help="Account to run tests under (default: %(default)s)",
    )
    parser.add_argument(
        "--activity",
        type=str,
        default="Functional Test",
        help="Activity to submit transfers (default: %(default)s)",
    )
    parser.add_argument(
        "--filesize",
        type=str,
        default="270MB",
        help="Size of load test files (default: %(default)s)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Verbosity",
    )

    args = parser.parse_args()

    loglevel = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    logging.basicConfig(
        format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
        level=loglevel[min(2, args.verbose)],
    )

    # UploadClient doesn't seem to pay attention to the client's account setting
    os.environ["RUCIO_ACCOUNT"] = args.account

    run(
        args.source_rse_expression,
        args.dest_rse_expression,
        args.account,
        args.activity,
        args.filesize,
    )