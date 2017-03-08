from ftplib import FTP
import os
import pprint
pp = pprint.PrettyPrinter(indent = 4)

import pyspark
from pyspark import  SparkContext

def get_contents_for_year(year):
    assert isinstance(year, int), "year must be int"
    ftp_url = 'ftp.ncdc.noaa.gov'
    with FTP(ftp_url) as ftp:
        ftp.login()
        the_dir = '/pub/data/noaa/{0}/'.format(year)
        ftp.cwd(the_dir)
        all_files = ftp.nlst()
        for the_file in all_files:
            with open(the_file, 'wb') as write_obj:
                ftp.retrbinary('RETR {0}'.format(the_file), write_obj.write)
            assert False

def test_for_each(r):
    print(dir(r))
    assert False

def _get_sc(test = False):
    if test:
        return SparkContext( 'local', 'pyspark')

def main():
    sc = _get_sc(True)
    #pp.pprint(dir(sc))
    rdd =   sc.parallelize(range(1901, 2017))
    #print(rdd.take(1))
    #pp.pprint(dir(rdd))
    rdd.foreach(test_for_each)


if __name__ == '__main__':
    main()
