import boto_emr.ftp_chunker
import tempfile
import boto3
import os
import json
import pprint
pp = pprint.PrettyPrinter(indent = 4)

def main():
    chunk_size = 1000000 * 1000 #10,000k
    my_gen  = boto_emr.ftp_chunker.make_chunks(chunk_size)
    for chunk_num, chunk in enumerate(my_gen):
        print("got chunk")
        f, tmp_file = tempfile.mkstemp()
        with open(tmp_file, 'w') as write_obj:
            write_obj.write(json.dumps(chunk))
        s3_client = boto3.client('s3')
        s3_client.upload_file(tmp_file, 'paulhtremblay', 'noaa_chunks/noaa_chunks_{0}.json'.format(chunk_num))
        os.close(f)
        os.remove(tmp_file)


if __name__ == '__main__':
    main()
