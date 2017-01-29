"""
Creates an EMR cluser with one master node and no slaves.
The cluster will terminate as soon it is spun up.

"""
import boto3
session = boto3.Session(profile_name='admin')
client = session.client('emr')
response = client.run_job_flow(
   Name = "tutorial example 1",
   ReleaseLabel = 'emr-5.2.1',
   Instances = {
       'InstanceGroups': [{
           'InstanceRole': 'MASTER',
           'Market': 'SPOT',
           'BidPrice': '.6',
           'InstanceType': 'm1.medium',
           'InstanceCount': 1
       }],
   },
   JobFlowRole = 'EMR_EC2_DefaultRole',
   ServiceRole = 'EMR_DefaultRole',
)
print(response)
