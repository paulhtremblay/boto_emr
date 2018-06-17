import boto3
import os

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
            key = contents['Key']
            yield key

def get_segments(bucket_name = 'commoncrawl', prefix =
        'crawl-data/CC-MAIN-2016-50/segments'):
    return list_objects(bucket_name = bucket_name, prefix = prefix)

def main():
    """
    get one file for testing
    """
    gen = list_objects(bucket_name = 'commoncrawl', prefix =
            'crawl-data/CC-MAIN-2016-50/segments')
    key = next(gen)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('commoncrawl')
    bucket.download_file(key, '/tmp/warc_ex.gz')


def get_crawl_paths(the_type, recursion_limit = None):
    assert the_type in ['crawldiagnostics']
    return list(filter(lambda x: os.path.split(os.path.split(x)[0])[1] == the_type,
            get_segments()))


if __name__ == '__main__':
    #main()
    l = get_crawl_paths(the_type = 'crawldiagnostics')
    print(len(l))
