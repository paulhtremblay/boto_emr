from ftplib import FTP
import pprint
pp = pprint.PrettyPrinter(indent = 4)

def _munge_fields(s, the_dict):
    fields = s.split('=')
    if len(fields) == 1:
        key = 'name'
        value = fields[0]
    else:
        key = fields[0]
        value = fields[1]
        if key == 'size':
            value = int(value)
    the_dict[key] = value

def get_paths(year):
    ls = _get_paths(year)
    the_dict = {}
    for l in ls:
        the_dict = {}
        fields = l.split(";")
        if fields[-1].strip() == '.' or fields[-1].strip() == '..':
            continue
        for field in fields:
            x = _munge_fields(field.strip(), the_dict)
        yield the_dict


def _get_paths(year):
    ftp_url = 'ftp.ncdc.noaa.gov'
    with FTP(ftp_url) as ftp:
        ftp.login()
        ls = []
        ftp.cwd('/pub/data/noaa/{0}/'.format(year))
        ftp.retrlines("MLSD", ls.append)
        return ls

def make_chunks(chunk_size):
    final = []
    temp  = []
    size = 0
    for year in range(1901, 2017):
        g = get_paths(year)
        size_of_chunk = 0
        for info in g:
            info['year'] = year
            size += info['size']
            if size > chunk_size:
                yield temp
                size = 0
                temp = []
            temp.append(info)

