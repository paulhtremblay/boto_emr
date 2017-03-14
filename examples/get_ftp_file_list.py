import boto_emr.ftp_chunker
import tempfile
import csv
import boto3
import os

def main():
    chunk_size = 1000000 * 1000 #10,000k
    my_gen  = boto_emr.ftp_chunker.make_chunks(chunk_size)
    chunk_list = []
    f, tmp_file = tempfile.mkstemp()
    with open(tmp_file, 'w') as write_obj:
        csv_writer = csv.writer(write_obj)
        csv_writer.writerow(['year', 'name', 'size', 'chunk_num'])
        for chunk_num, chunk in enumerate(my_gen):
            for d in chunk:
                csv_writer.writerow([d['year'], d['name'], d['size'], chunk_num])
    s3_client = boto3.client('s3')
    s3_client.upload_file(tmp_file, 'paulhtremblay', 'noaa_chunks.csv' )
    os.remove(tmp_file)


if __name__ == '__main__':
    main()
