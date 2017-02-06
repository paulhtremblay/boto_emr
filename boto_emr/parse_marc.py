import io
import datetime

def _get_record_start():
    return 'WARC/1.0'

def _get_key_value(line):
    f = line.split(':', 1)
    if len(f) == 2:
        return f[0].strip(), f[1].strip()
    return None, None

def make_date(s):
    return datetime.datetime.strptime(s[0:19], '%Y-%m-%dT%H:%M:%S')

def make_int(s):
    return int(s)

def make_string(s):
    return s

def _process_value(key, value):
    fun_dict = {'WARC-Date': make_date, 'Content-Length': make_int}
    return fun_dict.get(key, make_string)(value)

def _process_line(line, record, list_of_marc_records):
    if line.strip() == _get_record_start():
        list_of_marc_records.append(record)
        record = {}
    key, value =  _get_key_value(line)
    if key:
        record[key] = _process_value(key, value)

def parse(text):
    list_of_marc_records = []
    record = {}
    f = io.StringIO(text)
    line = True
    while line:
        line = f.readline()
        _process_line(line, record, list_of_marc_records)
    return list_of_marc_records
