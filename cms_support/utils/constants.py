# coding=utf-8

import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)


class Constants:
    VERSION = "0.0.1"
    AUTHOR = 'Pau-4quarks'
    CREDIT = 'CMS (Compact Muon Solenoid)\nCERN (European Organization for Nuclear Research)'
    EMAIL = '4quarks_cms@protonmail.com'
    PACKAGE_NAME = 'cms_support'
    URL_PROJECT = 'https://github.com/4quarks/CMS_Grid_Support/'

    AND = ' AND '
    AND_SIGN = '&'
    QUOTE = "\""
    INCLUDED = ".*{}.*"
    EXPRESSION = "/{}/"
    ADD_STR = "\"{}\""
    ADD_DATA = " AND {} data.{}:/.*{}.*/"
    ADD_NOT_DATA = " AND NOT data.{}:/.*{}.*/"
    NOT = "NOT"

    # COMMON
    REF_NUM_ERRORS = "num_errors"
    REF_TIMESTAMP = "timestamp"
    REF_TIMESTAMP_HR = "timestamp_hr"
    REF_DATA = "data"
    REF_SITE = "site"
    REF_HOST = "hostname"
    REF_FLAVOUR = "flavour"
    THRESHOLD_NLP = 0.99

    # EXCEL
    SEPARATION_COLUMNS = 2
    SEPARATION_ROWS = 4

    # TRANSFERS
    KEYWORDS_ERRORS = ['overwrite is not enabled', 'internal server error',
                       'no such file', 'timeout of 360 seconds has been exceeded',
                       'connection refused globus_xio', 'checksum mismatch',
                       'source and destination file size mismatch', 'protocol family not supported',
                       'permission_denied', 'srm_putdone error on the surl', 'no route to host',
                       "an end of file occurred", "stream ended before eod", 'handle not in the proper state',
                       'disk quota exceeded', 'received block with unknown descriptor', 'no data available',
                       'file has no copy on tape and no diskcopies are accessible',
                       'valid credentials could not be found',
                       'the server responded with an error 550 file not found'
                       'a system call failed: connection timed out', 'operation timed out', 'user timeout over',
                       'idle timeout: closing control connection',
                       'system error in write into hdfs', 'system error in reading from hdfs',
                       'reports could not open connection to', 'closing xrootd file handle',
                       'failed to read checksum file', 'SRM_NO_FREE_SPACE',
                       'invalid request type for token', 'IPC failed while attempting to perform request',
                       'copy failed with mode 3rd push', 'error accessing HOST', 'unknown error',
                       'unable to build the turl for the provided transfer protocol', 'Service Unavailable',
                       'connection reset by peer', 'site busy: too many queued requests',
                       'no fts server has updated the transfer status the last 900 seconds', "file is unavailable",
                       'file recreation canceled since the file cannot be routed to tape',
                       'checksum value required if mode is not end to end', 'login incorrect',
                       'unable to read replica', 'srm_file_busy', 'general problem: problem while connected',
                       'system error in connect: connection timed out', 'invalid request descriptor',
                       'failed to deliver poolmgrselectwritepoolmsg', 'a system call failed: broken pipe',
                       'no free space on storage area', 'commands denied', 'All pools are full',
                       'Changing file state because request state has changed', 'Protocol(s) not supported',
                       'The operation was aborted', 'Operation canceled', 'Connection limit exceeded', 'File not found',
                       'Not released because it is not pinned', 'Invalid argument',
                       'System error in mkdir', 'Internal HDFS error', 'Error recalling file from tape',
                       'StoRM encountered an unexpected error', 'Permission denied', 'Error reading token data header',
                       'Unexpected server error', 'SRM_INVALID_PATH', 'SRM_AUTHORIZATION_FAILURE',
                       'Network is unreachable', 'Connection timed out', "transfer forcefully killed"]

    REF_PFN_SRC = "src_url"
    REF_PFN_DST = "dst_url"
    REF_SE_SRC = "src_se"
    REF_SE_DST = "dst_se"
    REF_LFN_SRC = "src_lfn"
    REF_LFN_DST = "dst_lfn"
    REF_USER = "user"

    # LINKS
    SOURCE_KIBANA = "monit-kibana.cern.ch"
    SOURCE_KIBANA_ACC = "monit-kibana-acc.cern.ch"
    KIBANA = "https://{}/kibana/app/kibana#/discover?_g=(refreshInterval:(pause:!t,value:0)," \
             "time:(from:'{}.000Z',to:'{}.000Z'))&_a=(columns:!(_source),index:'{}',interval:auto," \
             "query:(language:lucene,query:'{}'),sort:!(metadata.timestamp,desc))"
    GRAFANA = "https://monit-grafana.cern.ch/api/datasources/proxy/{}/_msearch"


class CteRucio(Constants):
    REF_LOG = "purged_reason"
    REF_TRANSFER_ID = "transfer_id"

    INDEX_ES = "monit_prod_cms_rucio_enr_*"
    INDEX_ES_ID = "9732"


class CteFTS(Constants):
    REF_LOG = "reason"
    REF_JOB_ID = "job_id"
    REF_FILE_ID = "file_id"

    INDEX_ES = "monit_prod_fts_raw_*"
    INDEX_ES_ID = "9233"


class CteSAM(Constants):
    KEYWORD_SRM = ['SRM_INVALID_PATH', 'SRM_TOO_MANY_RESULTS', 'reports could not open connection to', 'METRIC FAILED',
                   'No such file or directory', 'System error in connect', "didn't send expected files",
                   'HTCondor-CE held job due to no matching routes, route job limit, or route failure threshold',
                   'Protocol(s) not supported']
    KEYWORD_CE = ['SYSTEM_PERIODIC_HOLD', 'New jobs are not allowed', 'No route to host', 'Killed CMSSW child',
                  'XRootDStatus.code=206 "[ERROR] Operation expired"', 'Unspecified gridmanager error',
                  'Local Stage Out Failed', 'Error sending files to schedd', 'Error connecting to schedd',
                  'LogicalFileNameNotFound', 'PERMISSION_DENIED', '[3011] No servers are available to read the file',
                  'Attempts to submit failed', 'Socket timed out on send/recv operation', 'BLAH_JOB_SUBMIT timed out',
                  'Redirect limit has been reached']
    KEYWORD_XRD = ['Ncat: Connection refused', 'Ncat: Connection timed out', 'invalid argument', 'Auth failed',
                   'Network is unreachable', 'Permission denied' 'Name or service not known']
    KEYWORDS_ERRORS = KEYWORD_SRM + KEYWORD_CE + KEYWORD_XRD

    NON_INTERESTING_TESTS = 'SE-xrootd-version|WN-cvmfs|DNS-IPv6|SRM-VOLsDir|SRM-VOLs|' \
                            'SRM-VODel|SRM-GetPFNFromTFC|SRM-VOGetTURLs|SRM-AllCMS'

    REF_LOG = "details"
    REF_LOG_CMSSST = "detail"
    REF_STATUS = "status"

    REF_SAM_METRIC = "sam"
    REF_SR_METRIC = "sts"

    REF_METRICS_SR = ["life_status", "prod_status", "crab_status", "manual_life", "manual_prod", "manual_crab"]

    # SITE STATUS
    NUM_ERRORS_ROW = 8
    HOURS_RANGE = 5
    NUM_TESTS_PER_HOUR = 4
    TOTAL_TESTS_EVALUATED = HOURS_RANGE * NUM_TESTS_PER_HOUR
    PERCENT_MIN_OK = 0.7  # 20 --> 14
    NUM_MIN_OK = int(HOURS_RANGE * NUM_TESTS_PER_HOUR * PERCENT_MIN_OK)

    INDEX_ES = "monit_prod_cmssst_*"
    INDEX_ES_ID = "9475"
