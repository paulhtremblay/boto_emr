import urllib.request
import tempfile
import os
import csv
import boto3

def _parse_html(html, year, data):
    for line in html.split(b'\r\n'):
        fields = line.split()
        if len(fields) != 9:
            continue
        data.append([year, fields[4].decode("utf8"), fields[8].decode("utf8")])

def _get_data_for_year(year, data_list):
    with urllib.request.urlopen('ftp://ftp.ncdc.noaa.gov/pub/data/noaa/{0}/'.format(year)) as response:
        html = response.read()
    _parse_html(html, year, data_list)

def _write_result_to_s3(data):
    s3_client = boto3.client('s3')
    fh, csv_path_file = tempfile.mkstemp()
    with open(csv_path_file, 'w') as write_obj:
        csv_writer = csv.writer(write_obj)
        for row in data:
            csv_writer.writerow(row)
    s3_client.upload_file(csv_path_file, 'paulhtremblay', 'noaa/index_ftp.csv')
    os.remove(csv_path_file)
    os.close(fh)

def main():
    data = [['year', 'bytes', 'path']]
    for year in range(1901, 2018):
        _get_data_for_year(year, data)
    _write_result_to_s3(data)

if __name__ == '__main__':
    main()
