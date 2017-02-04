
"""
For testing

"""
import datetime
import boto3
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
                   "fs.s3.consistent":"true",
                   "fs.s3.consistent.retryCount":"5",
                   "fs.s3.consistent.metadata.tableName":"EmrFSMetadata"},
               "Configurations":[]
               },
               ],
       },
           {
           'InstanceRole': 'CORE',
           'Market': 'SPOT',
           'BidPrice': '.6',
           'InstanceType': 'm1.medium',
           'InstanceCount': 1,
           'Configurations': [
               {"Classification":"emrfs-site",
               "Properties":{"fs.s3.consistent.retryPeriodSeconds":"10",
                   "fs.s3.consistent":"true",
                   "fs.s3.consistent.retryCount":"5",
                   "fs.s3.consistent.metadata.tableName":"EmrFSMetadata"},
               "Configurations":[]
               },
               ],



       }
           ],
       'Ec2KeyName': 'ubuntu-home',
       'KeepJobFlowAliveWhenNoSteps': True,
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
                'Name': 'ipython',
                'ScriptBootstrapAction': {
                    'Path': 's3://paulhtremblay/emr_bootstrap_ipython.sh',
                }
            },
        ]



response = client.run_job_flow(
   Name = "tutorial example {0}".format(datetime.datetime.now()),
   LogUri =  "s3n://paulhtremblay/emr-logs/",
   ReleaseLabel = 'emr-5.3.0',
   Instances = make_instances(),
   JobFlowRole = 'EMR_EC2_DefaultRole',
   ServiceRole = 'EMR_DefaultRole',
   BootstrapActions= make_bootstrap(), 
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
print(response)
