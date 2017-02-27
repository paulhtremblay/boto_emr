from pyspark.sql import Row
#import boto_emr.parse_marc as parse_marc
from pyspark import SparkContext
import datetime


def process_by_fields(l):
    host = None
    date = None
    text = None
    ip_address = None
    warc_type = None
    for line in l[1]:
        fields = line.split(':', 1)
        if fields and len(fields) == 2:
            if fields[0] == 'hostname':
                host = fields[1].strip()
            elif fields[0] == 'WARC-Date':
                date = fields[1].strip()
            elif fields[0] == 'WARC-IP-Address':
                ip_address = fields[1].strip()
            elif fields[0] == 'WARC-Type':
                warc_type = fields[1].strip()
        else:
            text = line
    return Row(host =host, date = date, text = text, 
            ip_address = ip_address, warc_type = warc_type)


def process_file(my_iter):
    the_id = "init"
    final = []
    for chunk in my_iter:
        lines = chunk[1].split("\n")
        for line in lines:
            if line[0:15] == 'WARC-Record-ID:':
                the_id = line[15:]
            final.append(Row(the_id = the_id, line = line))
    return iter(final)

def get_hdfs_path():
    return "/mnt/temp"


sc  = SparkContext(appName = "test crawl {0}".format(datetime.datetime.now()))
rdd = sc.wholeTextFiles(get_hdfs_path())\
        .mapPartitions(process_file)\
        .map(lambda x: (x.the_id, x.line))\
        .groupByKey()\
        .map(process_by_fields)
print(rdd.take(10))
