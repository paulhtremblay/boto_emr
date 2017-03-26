import boto3
import csv
import os
import tempfile
import argparse

def list_objects(bucket_name, prefix):
    final = []
    session = boto3.Session()
    client = session.client('s3')
    next_token = 'init'
    kwargs = {}
    while next_token:
        if next_token != 'init':
            kwargs.update({'ContinuationToken': next_token})
        response  = client.list_objects_v2(Bucket=bucket_name, Prefix = prefix,
                **kwargs)
        next_token = response.get('NextContinuationToken')
        for contents in response.get('Contents', []):
            yield contents

def make_full_s3_path(bucket, path_list):
    return 's3://' + join_keys(path_list)

def join_keys(path_list):
    final = ''
    for path in path_list:
        final = os.path.join(final, path)
    return final

def _write_result_to_s3(temp_path, valid_run):
    s3_client = boto3.client('s3')
    if valid_run:
        s3_client.upload_file(temp_path, 'paulhtremblay', 'noaa/tmp/index_s3.csv')
    else:
        s3_client.upload_file(temp_path, 'paulhtremblay', 'noaa/index_s3.csv')

def _get_args():
    parser = argparse.ArgumentParser(description='upload ftp files to S3')
    parser.add_argument('--test', action = 'store_true',
                help = 'test run on smaller data')
    parser.add_argument('--valid-run', action = 'store_true',
                help = 'make list for valid')
    return  parser.parse_args()


def main():
    args = _get_args()
    f, temp_path = tempfile.mkstemp()
    prefix = 'noaa'
    if args.valid_run:
        prefix = join_keys(['noaa', 'tmp', 'data_validation'])
    boto_list_gen = list_objects(bucket_name = 'paulhtremblay', prefix = prefix)
    str_years = [str(x) for x in range(1901, 2018)]
    with open(temp_path, 'w') as write_obj:
        csv_writer = csv.writer(write_obj)
        csv_writer.writerow(['year', 'bytes', 'path'])
        for counter, info in enumerate(boto_list_gen):
            if os.path.split(os.path.split(info['Key'])[0])[1] not in str_years\
                    or ('/tmp' in info['Key'] and not args.valid_run):
                continue
            if counter == 3 and args.test:
                break
            csv_writer.writerow(
                [
                    os.path.split(os.path.split(info['Key'])[0])[1],
                    info['Size'],
                    os.path.split(info['Key'])[1]
                ]
            )
    _write_result_to_s3(temp_path, args.valid_run)
    os.close(f)
    os.remove(temp_path)

if __name__ == '__main__':
    main()
