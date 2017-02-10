import io
import datetime

def _is_new_record(line):
    return line.strip() == b'WARC/1.0' or line.strip() == 'WARC/1.0'

def _get_key_value(line):
    if isinstance(line, bytes):
        f = line.split(b':', 1)
    else:
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
    if _is_new_record(line):
        list_of_marc_records.append(record)
        record = {}
    key, value =  _get_key_value(line)
    if key:
        #record[key] = _process_value(key, value)
        record[key] = value

def parse(text):
    list_of_marc_records = []
    record = {}
    if isinstance(text, bytes):
        f = io.BytesIO(text)
    else:
        f = io.StringIO(text)
    line = 'init'
    while line:
        line = f.readline()
        _process_line(line, record, list_of_marc_records)
    return list_of_marc_records
