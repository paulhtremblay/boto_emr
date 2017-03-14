import datetime
import boto3
import argparse
import sys

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
                    'Path': 's3://paulhtremblay/emr_bootstrap.sh',
                }
            },
            {
                'Name': 'python-hdfs',
                'ScriptBootstrapAction': {
                    'Path': 's3://paulhtremblay/emr_bootstrap_python.sh',
                }
            },
        ]



def create_response(start_year, end_year, local =False, test = False, write_dir = None, md5_sum = True):
    args = ['python34', '/usr/local/bin/upload_noaa1_to_s3.py']
    args.extend(['--start-year', str(start_year), '--end-year', str(end_year)])
    if test:
        args.append('--test')
    if md5_sum:
        args.append('--md5-sum')
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


def _get_args():
    parser = argparse.ArgumentParser(description='upload ftp files to S3')
    parser.add_argument('--test', action = 'store_true',
                help = 'test run on smaller data')
    parser.add_argument('--local', action = 'store_true',
                help = 'running on a machine without Hadoop')
    parser.add_argument('--validation', action = 'store_true',
                help = 'a validation run')
    parser.add_argument('--write-dir',
                help = 'directory to write to  dir (temporarily) ',
                default = "/mnt/years")
    parser.add_argument('--s3_dir', type = str,
            help ="s3 directory in the root directory", default = "noaa")
    parser.add_argument('--md5-sum', action = 'store_true',
                help = 'whether to write md5sum')
    args =  parser.parse_args()
    return args

def main():
    args = _get_args()
    years = [
            [1955,  1958],
            ]
    for year in years:
        response = create_response(start_year = year[0], end_year = year[1],
            local = args.local, test = args.test, write_dir = args.write_dir,
            md5_sum = True)
        print(response)

if __name__ == '__main__':
    main()
