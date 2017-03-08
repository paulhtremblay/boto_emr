#!/usr/bin/env python
from ftplib import FTP
import os
import pprint
pp = pprint.PrettyPrinter(indent = 4)
import argparse
import io
import subprocess
import shutil
import datetime
import hashlib

import pyspark
from pyspark import  SparkContext
from pyspark.sql import Row
import pyspark.sql


def md5(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def get_contents_for_year(year, max_num = None):
    """
    Not used

    """
    assert isinstance(year, int), "year must be int"
    ftp_url = 'ftp.ncdc.noaa.gov'
    final = []
    with FTP(ftp_url) as ftp:
        ftp.login()
        ftp.cwd('/pub/data/noaa/{0}/'.format(year))
        all_files = ftp.nlst()
        for file_num, path  in enumerate(all_files):
            if max_num and file_num >= max_num:
                break
            b_buffer = io.BytesIO()
            ftp.retrbinary('RETR {0}'.format(path), b_buffer.write)
            b_buffer.seek(0)
            final.append(b_buffer.read())
    return final

def make_root_dir_hdfs(root_dir, test = False):
    if test:
        return
    subprocess.call(["hadoop", "fs", '-mkdir', '-p',  root_dir])

def make_dirs_hdfs(year, root_dir):
    assert isinstance(year, int), "year must be int"
    new_dir = os.path.join(root_dir, str(year))
    subprocess.call(["hadoop", "fs", '-mkdir', '-p',  new_dir])

def make_dirs(year, root_dir):
    assert isinstance(year, int), "year must be int"
    new_dir = os.path.join(root_dir, str(year))
    if not os.path.exists(new_dir):
        os.makedirs(new_dir)

def move_to_hadoop(year, root_dir, local = False):
    from_dir = os.path.join(root_dir, str(year))
    if local:
        to_dir = os.path.join(root_dir, "{0}_".format(year))
        subprocess.call(["cp", "-R", from_dir, to_dir])
    else:
        subprocess.call(["hadoop", "fs", '-moveFromLocal', from_dir, root_dir])

def write_ftp_to_local(year, root_dir, max_num = None, local = False):
    assert isinstance(year, int), "year must be int"
    mdsums = []
    ftp_url = 'ftp.ncdc.noaa.gov'
    final = []
    with FTP(ftp_url) as ftp:
        ftp.login()
        ftp.cwd('/pub/data/noaa/{0}/'.format(year))
        all_files = ftp.nlst()
        for file_num, path  in enumerate(all_files):
            if max_num and file_num >= max_num:
                break
            local_gz = os.path.join(root_dir, str(year), path)
            with open(local_gz, 'wb') as write_obj:
                ftp.retrbinary('RETR {0}'.format(path), write_obj.write)
            mdsums.append((str(year), path, md5(local_gz)))
    move_to_hadoop(year, root_dir, local)
    return mdsums

def write_to_hadoop_and_make_checksum(my_iter, root_dir, max_num = None, local = False):
    """
    my_iter will be an iterator of years

    """
    final = []
    for year in my_iter:
        mdsums = write_ftp_to_local(year, root_dir, max_num, local)
        for the_mdsum in mdsums:
            final.append(Row(year = the_mdsum[0], mdsum = the_mdsum[2], file_name = the_mdsum[1]))
    return iter(final)

def _get_sc(test = False):
    if test:
        return SparkContext( 'local', 'pyspark')
    return SparkContext(appName = "ftp upload noaa data {0}".format(datetime.datetime.now()))

def _get_args():
    parser = argparse.ArgumentParser(description='upload ftp files to S3')
    parser.add_argument('--test', action = 'store_true',
                help = 'test run on smaller data')
    parser.add_argument('--local', action = 'store_true',
                help = 'running on a machine without Hadoop')
    parser.add_argument('--validation', action = 'store_true',
                help = 'a validation run')
    parser.add_argument('--root_dir', nargs = 1,
                help = 'root dir ',
                default = ["/mnt/years"])
    args =  parser.parse_args()
    return args

def main():
    args  = _get_args()
    sc = _get_sc(args.test)
    sqlContext = pyspark.SQLContext(sc)
    make_root_dir_hdfs(args.root_dir[0], args.test)
    if args.test:
        rdd =   sc.parallelize([2016])
    else:
        rdd =   sc.parallelize(range(1901, 2018))
    rdd.foreach(lambda x, root_dir = args.root_dir[0]: make_dirs(x, root_dir))
    if args.test:
        rdd = rdd.mapPartitions(lambda x, root_dir = args.root_dir[0], max_num = 3, local =
                args.local:
                write_to_hadoop_and_make_checksum(x, root_dir, max_num, local))
    else:
        rdd.foreach(lambda x, root_dir = args.root_dir[0]: write_ftp_to_local(x, root_dir))

        rdd = rdd.mapPartitions(lambda x, root_dir = args.root_dir[0]:
                write_to_hadoop_and_make_checksum(x, root_dir))
    s3_mdsum_dir = "s3://paulhtremblay/md5_noaa"
    if args.validation:
        s3_mdsum_dir = "s3://paulhtremblay/md5_noaa_validation"
    rdd.toDF().write.csv(s3_mdsum_dir)

if __name__ == '__main__':
    main()
