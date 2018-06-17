import io
import datetime

def _is_new_record(line):
    return  line.strip() == 'WARC/1.0'

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
    if _is_new_record(line):
        list_of_marc_records.append(record)
        record = {}
    key, value =  _get_key_value(line)
    if key:
        record[key] = _process_value(key, value)

def parse_record(text):
    f = io.StringIO(text)
    line = 'init'
    record = {'header':{}, 'body':{}}
    part = 'header'
    while line:
        line = f.readline()
        if line.strip() == '':
            part = 'start_body'
            continue
        if part == 'start_body':
            record['body']['response'] = line.strip()
            part = 'body'
            continue
        key, value = _get_key_value(line)
        if key:
            record[part][key] = _process_value(key, value)
    return record

def parse(text):
    list_of_marc_records = []
    record = {}
    f = io.StringIO(text)
    line = 'init'
    while line:
        line = f.readline()
        _process_line(line, record, list_of_marc_records)
    return list_of_marc_records
