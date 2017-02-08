import boto3
import gzip
import io
session = boto3.Session(profile_name='admin')
client = session.client('s3')
bucket_name = 'paulhtremblay'
encoding = 'utf8'
contents = gzip.GzipFile(
        fileobj= io.BytesIO(client.get_object(Bucket = bucket_name, Key = 'date.txt.gz')['Body'].read()))\
                .read()\
                .decode(encoding = encoding)
print(contents)
