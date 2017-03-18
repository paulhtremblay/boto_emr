#!/usr/bin/env python
from ftplib import FTP
import os
import pprint
import argparse
import datetime
import hashlib
import boto3
import csv
import tempfile
import json
import random
import string
pp = pprint.PrettyPrinter(indent = 4)

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


def _generate_random(length = 5):
    return ''.join(random.choice(string.ascii_lowercase + string.ascii_uppercase) for i in range(length))


def write_ftp_to_local(json_objects,  s3_dir, mdsums,  validation = False):
    s3_client = boto3.client('s3')
    with FTP('ftp.ncdc.noaa.gov') as ftp:
        ftp.login()
        for json_obj in json_objects:
            ftp.cwd('/pub/data/noaa/{0}/'.format(json_obj["year"]))
            fh, local_gz = tempfile.mkstemp()
            with open(local_gz, 'wb') as write_obj:
                ftp.retrbinary('RETR {0}'.format(json_obj["name"]), write_obj.write)
            mdsums.append((str(json_obj["year"]), json_obj["name"], md5(local_gz )))
            s3_out_dir = '{0}/{1}/{2}'.format(s3_dir, json_obj["year"],
                    json_obj["name"])
            s3_client.upload_file(local_gz, 'paulhtremblay', s3_out_dir )
            os.close(fh)
            os.remove(local_gz)

def _get_args():
    parser = argparse.ArgumentParser(description='upload ftp files to S3')
    parser.add_argument('--test', action = 'store_true',
                help = 'test run on smaller data')
    parser.add_argument('--validation', action = 'store_true',
                help = 'a validation run')
    parser.add_argument('--s3_dir', type = str,
            help ="s3 directory in the root directory", default = "noaa")
    parser.add_argument('--chunk-num', type = int,
            help ="chunk number to work on")
    args =  parser.parse_args()
    return args

def read_json(path):
    with open(path) as read_obj:
        return json.load(read_obj)


def main():
    args  = _get_args()
    s3_client = boto3.client('s3')
    bucket = 'paulhtremblay'
    f, path_file = tempfile.mkstemp()
    s3_client.download_file(bucket,
            'noaa_chunks/noaa_chunks_{0}.json'.format(args.chunk_num), path_file)
    chunk = read_json(path_file)
    s3_dir = args.s3_dir
    if args.validation:
        s3_dir += '_validation'
    mdsums = []
    write_ftp_to_local(json_objects = chunk,  s3_dir = s3_dir, mdsums = mdsums,
            validation = args.validation)
    s3_mdsum_path =\
            "md5_noaa_{0}_{1}.csv"\
            .format(datetime.datetime.now()\
            .strftime("%Y_%m_%d_%H_%M_%S"),
            _generate_random())
    s3_md_sum_dir = 'noaa_mdsums'
    if args.validation:
        s3_md_sum_dir = 'noaa_mdsums_validation'
    f, csv_path_file = tempfile.mkstemp()
    with open(csv_path_file, 'w') as write_obj:
        csv_writer = csv.writer(write_obj)
        for mdsum in mdsums:
            csv_writer.writerow(mdsum)
    s3_client.upload_file(csv_path_file, 'paulhtremblay',
            '{0}/{1}'.format(s3_md_sum_dir, s3_mdsum_path))
    os.remove(path_file)
    os.remove(csv_path_file)

if __name__ == '__main__':
    main()
