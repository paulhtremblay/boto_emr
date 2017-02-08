import boto3
import io
import gzip
from pyspark.sql import Row
import boto_emr.parse_marc as parse_marc
from pyspark import SparkContext

def process_file(s):
    return [Row(**x) for x in parse_marc.parse(s)]

def get_read_bucket(test = False):
    if test:
        return 'paulhtremblay'
    return 'commoncrawl'

def get_in_files(test = False):
    if test:
        return ['test/warc_crawl1.txt.gz', 'test/warc_crawl2.txt.gz', 'test/warc_crawl3.txt.gz']
    return ['crawl-data/CC-MAIN-2016-50/segments/1480698542588.29/crawldiagnostics/CC-MAIN-20161202170902-00507-ip-10-31-129-80.ec2.internal.warc.gz']

def get_contents(key, bucket_name, encoding= "utf8"):
    session = boto3.Session()
    client = session.client('s3')
    return  client.get_object(Bucket = bucket_name, Key = key)['Body'].read()

def my_gunzip(b):
    fileobj= io.BytesIO(b)
    return gzip.GzipFile(fileobj = fileobj).read().decode()

test = True
sc  = SparkContext(appName = "test crawl 1")
rdd = sc.parallelize(get_in_files(test = test))\
        .map(lambda x, bucket_name = get_read_bucket(test = test): get_contents(bucket_name = bucket_name, key = x))\
        .map(my_gunzip)\
        .map(process_file)\
        .flatMap(lambda x:x)
