
class Constants:
    AND = ' AND '
    AND_SIGN = '&'
    QUOTE = "\""
    INCLUDED = ".*{}.*"
    EXPRESSION = "/{}/"
    ADD_DATA = " AND data.{}:/.*\"{}\".*/"

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

    # SITE STATUS
    NUM_ERRORS_ROW = 8
    HOURS_RANGE = 5
    NUM_TESTS_PER_HOUR = 4
    TOTAL_TESTS_EVALUATED = HOURS_RANGE * NUM_TESTS_PER_HOUR
    PERCENT_MIN_OK = 0.7  # 20 --> 14
    NUM_MIN_OK = int(HOURS_RANGE * NUM_TESTS_PER_HOUR * PERCENT_MIN_OK)


class CteFTS(Constants):
    # TRANSFERS
    KEYWORDS_ERRORS = ['srm_authorization_failure', 'overwrite is not enabled', 'internal server error',
                           'no such file or directory', 'timeout of 360 seconds has been exceeded',
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
                           'failed to read checksum file',
                           'invalid request type for token', 'error reading token data header',
                           'copy failed with mode 3rd push',
                           'unable to build the turl for the provided transfer protocol',
                           'a system call failed: connection reset by peer', 'site busy: too many queued requests',
                           'no fts server has updated the transfer status the last 900 seconds', "file is unavailable",
                           'file recreation canceled since the file cannot be routed to tape',
                           'checksum value required if mode is not end to end', 'login incorrect',
                           'unable to read replica', 'srm_file_busy', 'general problem: problem while connected',
                           'system error in connect: connection timed out', 'invalid request descriptor',
                           'failed to deliver poolmgrselectwritepoolmsg', 'a system call failed: broken pipe',
                           'no free space on storage area', 'commands denied']
    REF_PFN_SRC = "src_url"
    REF_PFN_DST = "dst_url"
    REF_SE_SRC = "source_se"
    REF_SE_DST = "dest_se"
    REF_LFN_SRC = "src_lfn"
    REF_LFN_DST = "dst_lfn"
    REF_LOG = "reason"
    REF_JOB_ID = "job_id"
    REF_FILE_ID = "file_id"
    REF_USER = "user"

    INDEX_ES = "monit_prod_fts_raw_*"
    INDEX_ES_ID = "9233"

    # INDEX_ES = "monit_prod_cms_rucio_enr*"
    # INDEX_ES_ID = "9732"


class CteSAM(Constants):
    KEYWORDS_ERRORS = ["no servers are available to read the file", "ncat: connection refused",
                          'cream_delegate error', 'operation not permitted']
    REF_LOG = "details"
    REF_LOG_CMSSST = "detail"
    REF_STATUS = "status"

