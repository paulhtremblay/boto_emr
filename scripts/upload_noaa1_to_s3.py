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

def md5(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def _generate_random(length = 5):
    return ''.join(random.choice(string.ascii_lowercase + string.ascii_uppercase) for i in range(length))

def _upload_contents_to_s3(ftp, s3_client, json_obj, local_gz, mdsums, s3_out_dir, bucket):
    ftp.cwd('/pub/data/noaa/{0}/'.format(json_obj["year"]))
    with open(local_gz, 'wb') as write_obj:
        ftp.retrbinary('RETR {0}'.format(json_obj["name"]), write_obj.write)
    mdsums.append((str(json_obj["year"]), json_obj["name"], md5(local_gz )))
    #s3_client.upload_file(local_gz, bucket, s3_out_dir )

def _handle_error(ex, json_obj, errors):
    message = "excpetion: {0}, args: {1}".format(type(ex).__name__, ', '.join(ex.args))
    errors.append((json_obj["year"], json_obj["name"], message))

def write_ftp_to_local(json_objects,  s3_dir, mdsums, errors, bucket, s3_client, validation = False, test = False):
    with FTP('ftp.ncdc.noaa.gov') as ftp:
        ftp.login()
        for counter, json_obj in enumerate(json_objects):
            fh, local_gz = tempfile.mkstemp()
            s3_out_dir = '{0}/{1}/{2}'.format(s3_dir, json_obj["year"],
                json_obj["name"])
            try:
                if test and counter == 5:
                    raise ValueError("Test throwing a value error") #for testing
                _upload_contents_to_s3(ftp = ftp, s3_client = s3_client, json_obj = json_obj,
                        local_gz = local_gz, mdsums = mdsums, s3_out_dir = s3_out_dir, bucket = bucket)
            except Exception as ex:
                _handle_error(ex, json_obj, errors)
            finally:
                os.close(fh)
                os.remove(local_gz)
            if test and counter == 10:
                break

def read_json(path):
    with open(path) as read_obj:
        return json.load(read_obj)

def _get_s3_md_sum_dir(validation):
    if validation:
        return  os.path.join(
                _get_tmp_dir(), 'noaa_mdsums_validation')
    return  os.path.join(
                _get_root_noaa_dir(), 'noaa_mdsums')

def _get_s3_md5sum_path():
    return "md5_{0}_{1}.csv"\
            .format(datetime.datetime.now()\
            .strftime("%Y_%m_%d_%H_%M_%S"),
            _generate_random())

def _write_mdsum_to_s3(mdsums, validation, s3_client, bucket):
    fh, csv_path_file = tempfile.mkstemp()
    with open(csv_path_file, 'w') as write_obj:
        csv_writer = csv.writer(write_obj)
        for mdsum in mdsums:
            csv_writer.writerow(mdsum)
    s3_client.upload_file(csv_path_file, bucket,
            '{0}/{1}'.format(_get_s3_md_sum_dir(validation),
                _get_s3_md5sum_path()))
    os.remove(csv_path_file)
    os.close(fh)

def _get_root_noaa_dir():
    return 'noaa'

def _get_tmp_dir():
    return os.path.join(_get_root_noaa_dir(), "tmp")

def _get_chunks_dir():
    return os.path.join(_get_tmp_dir(), 'chunks')

def _get_chunks_from_s3(chunk_num, s3_client, bucket):
    fh, path_file = tempfile.mkstemp()
    s3_client.download_file(bucket,
            os.path.join(
                _get_chunks_dir(),
                'noaa_chunks_{0}.json'\
                .format(chunk_num)),
            path_file
            )
    chunk =  read_json(path_file)
    os.close(fh)
    os.remove(path_file)
    return chunk

def _get_s3_dir(args):
    if args.validation:
        return os.path.join("noaa", "tmp", "data_validation")
    return os.path.join("noaa", "data")

def _write_errors_to_s3(errors, s3_client, bucket):
    fh, csv_path_file = tempfile.mkstemp()
    with open(csv_path_file, 'w') as write_obj:
        csv_writer = csv.writer(write_obj)
        for error in errors:
            csv_writer.writerow(error)
    s3_client.upload_file(csv_path_file, bucket,
            os.path.join(_get_tmp_dir(), "upload_error_{0}.csv".format(datetime.datetime.now()))
            )
    os.remove(csv_path_file)
    os.close(fh)

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
    parser.add_argument('--bucket', type = str,
            help ="bucket to write results to", default = "paulhtremblay")
    args =  parser.parse_args()
    return args

def main():
    args  = _get_args()
    s3_client = boto3.client('s3')
    mdsums, errors = [], []
    write_ftp_to_local(json_objects = _get_chunks_from_s3(
                chunk_num = args.chunk_num, s3_client = s3_client, bucket = args.bucket),
            s3_dir = _get_s3_dir(args), mdsums = mdsums,
            errors = errors, validation = args.validation,
            bucket = args.bucket, s3_client = s3_client, test = args.test)
    _write_mdsum_to_s3(mdsums, args.validation, s3_client, args.bucket)
    _write_errors_to_s3(errors, s3_client, args.bucket)

if __name__ == '__main__':
    main()
