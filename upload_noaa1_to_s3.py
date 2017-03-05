from ftplib import FTP
import os

def get_contents(year):
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

if __name__ == '__main__':
    get_contents(2000)
