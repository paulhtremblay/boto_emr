import json
import pprint
import csv
pp = pprint.PrettyPrinter(indent = 4)
import tempfile
import boto3
import os
import re

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

def _get_last_chunk_num(bucket):
    reg_exp = re.compile(r'noaa_chunks_(\d+)\.json')
    my_gen = list_objects(bucket, 'noaa/tmp/chunks')
    the_max = 0
    for info in my_gen:
        path = info['Key']
        head, tail = os.path.split(path)
        if not tail:
            continue
        num = reg_exp.match(tail)
        if not num:
            continue
        num = int(num.group(1))
        if num > the_max:
            the_max= num
    return the_max

def upload_to_s3(local_path, chunk_num, bucket):
    s3_client = boto3.client('s3')
    s3_client.upload_file(local_path, bucket,
            'noaa/tmp/chunks/noaa_chunks_{0}.json'.format(chunk_num))

def make_chunk(local_path, chunk_num, bucket):
    final = []
    with open(local_path, "r") as read_obj:
        csv_reader = csv.reader(read_obj)
        for row in csv_reader:
            final.append(
                {'UNIX.group': '4021',
                'UNIX.mode': '0644',
                'UNIX.owner': '4021',
                'modify': '20041119065552',
                'name': row[2],
                'perm': 'adfr',
                'size': 9708,
                'type': 'file',
                'unique': '46U2C156F8',
                'year':row[0]
                }
                    )
    fh, temp_path = tempfile.mkstemp()
    with open(temp_path, "w") as write_obj:
        json.dump(final, write_obj)
    upload_to_s3(temp_path, chunk_num, bucket)

    os.close(fh)
    os.remove(temp_path)

def look_at_json():
    with open('/home/paul/Downloads/noaa_chunks_94.json', 'r') as write_obj:
        data = json.load(write_obj)
    pp.pprint(data)



def main():
    look_at_json()
    return
    bucket = 'paulhtremblay'
    last_chunk_num = _get_last_chunk_num(bucket)
    for counter, path in enumerate(["/home/paul/Downloads/main_valid.csv",
            "/home/paul/Downloads/second_valid.csv"]):
        make_chunk(path, last_chunk_num + counter + 1, bucket)


if __name__ == '__main__':
    main()

