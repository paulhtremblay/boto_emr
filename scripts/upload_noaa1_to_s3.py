#!/usr/bin/env python
from ftplib import FTP
import os
import pprint
pp = pprint.PrettyPrinter(indent = 4)
import argparse
import datetime
import hashlib
import boto3
import csv

def s3_delete(key):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket("paulhtremblay")
    for obj in list(bucket.objects.filter(Prefix=key)):
        obj.delete()

def md5(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def write_ftp_to_local(year, root_dir, s3_dir, mdsums, max_num = None, local =
        False, validation = False, make_md5sums = True):
    assert isinstance(year, int), "year must be int"
    ftp_url = 'ftp.ncdc.noaa.gov'
    new_dir = os.path.join(root_dir, str(year))
    if not os.path.exists(new_dir):
        os.makedirs(new_dir)
    s3_client = boto3.client('s3')
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
            md5_sum = ''
            if make_md5sums:
                md5(local_gz)
            mdsums.append((str(year), path, md5_sum ))
            s3_out_dir = '{0}/{1}/{2}'.format(s3_dir, year, path)
            if validation:
                s3_out_dir = '{0}_validation/{1}/{2}'.format(s3_dir, year, path)
            s3_client.upload_file(local_gz, 'paulhtremblay', s3_out_dir )

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
    parser.add_argument('--write-dir', 
                help = 'write direcotry ',
                default = "/mnt/years")
    parser.add_argument('--start-year', type = int, required = True,
            help ="year to start data")
    parser.add_argument('--end-year', type = int, required = True,
            help ="year to end data")
    parser.add_argument('--s3_dir', type = str,
            help ="s3 directory in the root directory", default = "noaa")
    parser.add_argument('--md5-sum', action = 'store_true',
                help = 'create md5-sums')
    args =  parser.parse_args()
    return args

def main():
    args  = _get_args()
    s3_dir = args.s3_dir
    if args.validation:
        s3_dir += '_validation'
    years = range(args.start_year, args.end_year + 1)
    mdsums = []
    max_num = None
    if args.test:
        max_num = 3
    s3_mdsum_path = "md5_noaa.csv"
    s3_client = boto3.client('s3')
    s3_md_sum_dir = 'noaa_mdsums'
    if args.validation:
        s3_md_sum_dir = 'noaa_mdsums_validation'
    for year in years:
        write_ftp_to_local(year, args.write_dir, s3_dir, mdsums, max_num,
                args.local, args.validation, args.md5_sum)
        with open('temp.csv', 'w') as write_obj:
            csv_writer = csv.writer(write_obj)
            for mdsum in mdsums:
                csv_writer.writerow(mdsum)
        s3_client.upload_file('temp.csv', 'paulhtremblay',
                '{0}/{1}/{2}'.format(s3_md_sum_dir, year, s3_mdsum_path))

if __name__ == '__main__':
    main()
