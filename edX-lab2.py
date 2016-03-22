# Description Log files stuff

import re
import datetime
import sys
import os
from operator import add, div

#--------- Spark setup -----------
# Configure the environment for PySpark
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/opt/spark'
# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']
# Add the PySpark/py4j to the Python Path
sys.path.insert(0, os.path.join(SPARK_HOME, "python", "lib"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
from pyspark.sql import Row
from pyspark import SparkContext
sc = SparkContext("local", "Lab2")
#--------- Spark setup -----------


APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}


def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),  # Day
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)


def parseLogs( log_file ):
    """ Read and parse log file """
    parsed_logs = (sc
                   .textFile( log_file )
                   .map(parseApacheLogLine)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs


def emulate_map(log_path):
    f = open(log_path)
    rows = []
    for line in f.readlines():
        rows.append(parseApacheLogLine(line))
    return rows


if __name__ == '__main__':
    # baseDir = os.path.join('data')
    # inputPath = os.path.join('cs100', 'lab2', 'apache.access.log.PROJECT')
    inputPath = 'shortlog.log'
    # # logFile = os.path.join(baseDir, inputPath)
    logFile = inputPath
    parsed_logs, access_logs, failed_logs = parseLogs(logFile)

    # Count URLs in erro (not code 200)
    not200 = access_logs.map(lambda log: log).filter(lambda log: log.response_code != 200)
    endpointCountPairTuple = not200.map(lambda log: (log.endpoint, 1))
    endpointSum = endpointCountPairTuple.reduceByKey(add)
    topTenErrURLs = endpointSum.takeOrdered(10, lambda (k, v): -v)
    print 'Top Ten failed URLs: %s' % topTenErrURLs

    # Number of unique Hosts
    hosts = access_logs.map(lambda log: log.host)
    uniqueHosts = hosts.distinct()
    uniqueHostCount = uniqueHosts.count()
    print 'Unique hosts: %d' % uniqueHostCount

    # Number of Unique Daily Hosts
    dayToHostPairTuple = access_logs.map(lambda log: (log.date_time.day, log.host)).distinct()
    dayGroupedHosts = dayToHostPairTuple.groupBy(lambda (x,y): x)
    dayHostCount = dayGroupedHosts.map(lambda (day, hosts): (day, len(hosts)))
    dailyHosts = (dayHostCount.sortByKey().cache())
    dailyHostsList = dailyHosts.take(30)
    print 'Unique hosts per day: %s' % dailyHostsList
    print "Mean unique hosts per day: {}".format(dailyHosts.map(lambda (d,c): c).mean())

    # Average Number of Daily Requests per Hosts
    dayAndHostTuple = access_logs.map(lambda log: (log.date_time.day, log.host))
    groupedByDay = dayAndHostTuple.groupBy(lambda (x,y): x)
    sortedByDay = groupedByDay.sortByKey()
    avgDailyReqPerHost = (sortedByDay
                         .map(lambda (day, host_tuples): (day, len(host_tuples) / len(set(host_tuples))))
                         .cache())
    avgDailyReqPerHostList = avgDailyReqPerHost.take(30)
    print 'Average number of daily requests per Hosts is %s' % avgDailyReqPerHostList

    # Get all 404 records
    badRecords = (access_logs
              .filter(lambda log: log.response_code == 404)
              .cache())
    print 'Found %d 404 URLs' % badRecords.count()
    badEndpoints = badRecords.map(lambda log: log.endpoint)
    badUniqueEndpoints = badEndpoints.distinct()
    badUniqueEndpointsPick40 = badUniqueEndpoints.take(40)
    print '404 URLS: %s' % badUniqueEndpointsPick40

    # List top twenty 404 response code endpoints
    badEndpointsCountPairTuple = badRecords.map(lambda log: (log.endpoint, 1))
    badEndpointsSum = badEndpointsCountPairTuple.reduceByKey(add)
    badEndpointsTop20 = badEndpointsSum.takeOrdered(20, lambda (k, v): -v)
    print 'Top Twenty 404 URLs: %s' % badEndpointsTop20

    # Listing 404 Response Codes per Day
    errDateCountPairTuple = badRecords.map(lambda log: (log.date_time.day, 1))
    errDateSum = errDateCountPairTuple.reduceByKey(add)
    errDateSorted = (errDateSum.sortByKey().cache())
    errByDate = errDateSorted.collect()
    print '404 Errors by day: %s' % errByDate

    # Visualizing the 404 Response Codes by Day
    daysWithErrors404 = errDateSorted.map(lambda (d, c): d).collect()
    errors404ByDay = errDateSorted.map(lambda (d, c): c).collect()
    fig = plt.figure(figsize=(8,4.2), facecolor='white', edgecolor='white')
    plt.axis([0, max(daysWithErrors404), 0, max(errors404ByDay)])
    plt.grid(b=True, which='major', axis='y')
    plt.xlabel('Day')
    plt.ylabel('404 Errors')
    plt.plot(daysWithErrors404, errors404ByDay)
    pass

    # Top Five Days for 404 Response Codes
    topErrDate = errDateSorted.takeOrdered(5, lambda (k, v): -v)
    print 'Top Five dates for 404 requests: %s' % topErrDate

    # Hourly 404 response codes
    hourCountPairTuple = badRecords.map(lambda log: (log.date_time.hour, 1))
    hourRecordsSum = hourCountPairTuple.reduceByKey(add)
    hourRecordsSorted = (hourRecordsSum.sortByKey().cache())
    errHourList = hourRecordsSorted.collect()
    print 'Top hours for 404 requests: %s' % errHourList


    do = True
    # host          = match.group(1),
    # client_identd = match.group(2),
    # user_id       = match.group(3),
    # date_time     = parse_apache_time(match.group(4)),
    # method        = match.group(5),
    # endpoint      = match.group(6),
    # protocol      = match.group(7),
    # response_code = int(match.group(8)),
    # content_size  = size


    # emulate_map(logFile)



