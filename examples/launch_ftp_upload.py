import datetime
import boto3
import argparse
import sys
import time
import random

session = boto3.Session(profile_name='admin')
client = session.client('emr')

def make_instances():
    return {
       'InstanceGroups': [
           {
           'InstanceRole': 'MASTER',
           'Market': 'SPOT',
           'BidPrice': '.6',
           'InstanceType': 'm1.medium',
           'InstanceCount': 1,
           'Configurations': [
               {"Classification":"emrfs-site",
               "Properties":{"fs.s3.consistent.retryPeriodSeconds":"10",
                   "consistent":"true",
                   "Consistent":"true",
                   "fs.s3.consistent.retryCount":"5",
                   "fs.s3.consistent.metadata.tableName":"EmrFSMetadata"},
               "Configurations":[]
               },
               ],
       },
           ],
       #'Ec2KeyName': 'ubuntu-home',
       'KeepJobFlowAliveWhenNoSteps': False,
       'Ec2SubnetId': "subnet-0f159079",
   }

def make_bootstrap():
    return[
            {
                'Name': 'Generic bootstrap',
                'ScriptBootstrapAction': {
                    'Path': 's3://paulhtremblay/emr_bootstraps/emr_bootstrap.sh',
                }
            },
            {
                'Name': 'python-hdfs',
                'ScriptBootstrapAction': {
                    'Path': 's3://paulhtremblay/emr_bootstraps/emr_bootstrap_python.sh',
                }
            },
        ]



def create_response(chunk_num, test = False, validation = False):
    args = ['python34', '/usr/local/bin/upload_noaa1_to_s3.py']
    args.extend(['--chunk-num', str(chunk_num)])
    if test:
        args.append('--test')
    if validation:
        args.append('--validation')
    response = client.run_job_flow(
       Name = "ftp upload example {0}".format(datetime.datetime.now()),
       LogUri =  "s3n://paulhtremblay/emr-logs/",
       ReleaseLabel = 'emr-5.3.0',
       Instances = make_instances(),
       JobFlowRole = 'EMR_EC2_DefaultRole',
       ServiceRole = 'EMR_DefaultRole',
       BootstrapActions= make_bootstrap(),
       Steps=[{'HadoopJarStep': {'Args':  args,
           'Jar': 'command-runner.jar'},
           'Name': 'simple step', 'ActionOnFailure': 'TERMINATE_CLUSTER'}],
       Applications =  [
            {
                "Name": "Hadoop",
            },
            {
                "Name": "Spark",
            }
        ],

        Configurations = [{
            'Classification': 'spark-log4j',
            'Properties': {
                'log4j.rootCategory': 'ERROR, console'
            }
        }, {
            'Classification': 'spark-defaults',
            'Properties': {
                'spark.ui.showConsoleProgress': 'false'
            }
        }, {
            'Classification': 'spark-env',
            'Properties': {},
            'Configurations': [{
                'Classification': 'export',
                'Properties': {
                    'PYSPARK_PYTHON': 'python34',
                    'PYSPARK_DRIVER_PYTHON': 'python34'
                }
            }]
        }],
       )
    return response

def test_run(start_year, end_year, local =False, test = False, write_dir = None, md5_sum = True):
    import subprocess
    args = ['python3', '/home/paul/Documents/projects/emr_admin/boto_emr/scripts/upload_noaa1_to_s3.py']
    args.extend(['--start-year', str(start_year), '--end-year', str(end_year)])
    if test:
        args.append('--test')
    if write_dir:
        args.extend(['--write-dir', write_dir])
    if md5_sum:
        args.append('--md5-sum')
    subprocess.call(args)

def _get_last_chunk():
    return 93

def _num_clusters_free(max_num_emr_clusters):
    s3_client = boto3.client("emr")
    return max_num_emr_clusters\
            - len(list(filter(lambda x: x['State'][0:10] != 'TERMINATED', [x['Status'] for x in s3_client.list_clusters()['Clusters']])))


def _get_args():
    parser = argparse.ArgumentParser(description='upload ftp files to S3')
    parser.add_argument('--test', action = 'store_true',
                help = 'test run on smaller data')
    parser.add_argument('--validation', action = 'store_true',
                help = 'a validation run')
    parser.add_argument('--s3_dir', type = str,
            help ="s3 directory in the root directory", default = "noaa")
    args =  parser.parse_args()
    return args

def main():
    args = _get_args()
    print(create_response(chunk_num = 95, test = False, validation = True))
    return
    start = 0
    max_num_emr_clusters = 18
    end = start + max_num_emr_clusters + 1
    last_chunk_num = _get_last_chunk()
    sleep_time = 60 * 25
    while 1:
        for chunk_num in range(start, end):
            print("start up emr cluster with args of chunk_num {0}".format(chunk_num))
            print(create_response(chunk_num = chunk_num, test = args.test, validation = args.validation))
        if end > last_chunk_num:
            break
        while 1:
            time.sleep(sleep_time)
            num_clusters_free = _num_clusters_free(max_num_emr_clusters)
            if num_clusters_free > 0:
                start = end
                end = start + num_clusters_free + 1
                if end > last_chunk_num:
                    end = last_chunk_num + 1
                break

if __name__ == '__main__':
    main()
