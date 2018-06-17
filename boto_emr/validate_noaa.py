from pyspark.sql import Row
from pyspark import SparkContext
import datetime
from  pyspark.sql import SQLContext
import datetime
import hashlib
import os
import boto3
import StringIO
import csv

def s3_delete(key):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket("paulhtremblay")
        for obj in list(bucket.objects.filter(Prefix=key)):
            obj.delete()

def get_md5_sum_string(s, chunk_size = 5000):
    start = 0
    end = chunk_size
    hash_md5 = hashlib.md5()
    while 1:
        hash_md5.update(s[start:end].encode("utf8"))
        if end > len(s):
            break
        start = end
        end = start + chunk_size
    return hash_md5.hexdigest()

def get_md5sum(my_iter):
    final = []
    for line in my_iter:
        md5_sum = get_md5_sum_string(line[1])
        final.append(Row(md5sum = md5_sum, path = line[0], key = os.path.split(line[0])[1]))
    return iter(final)

def make_rdd(sc, start_year, end_year, validation = False):
    for counter, year in enumerate(range(start_year, end_year + 1)):
        rdd_temp = sc.wholeTextFiles("s3://paulhtremblay/noaa/tmp/data_validation/{0}/".format(year), 300)
        if counter == 0:
            rdd = rdd_temp
        else:
            rdd = rdd.union(rdd_temp)
    return rdd


def make_rdd(sc, start_year, end_year, validation = False):
    for counter, year in enumerate(range(start_year, end_year + 1)):
        if validation:
            rdd_temp = sc.wholeTextFiles("s3://paulhtremblay/noaa/tmp/data_validation/{0}/".format(year))\
                .mapPartitions(get_md5sum)
        else:
            rdd_temp = sc.wholeTextFiles("s3://paulhtremblay/noaa/data/{0}/".format( year))\
                .mapPartitions(get_md5sum)
        if counter == 0:
            rdd = rdd_temp
        else:
            rdd = rdd.union(rdd_temp)
    return rdd

def md5sums_match(r):
    if len(r[1]) != 2:
        return False
    return r[1].data[0][1] == r[1].data[1][1]

def md5sums_no_match(r):
    if len(r[1]) != 2:
        return True
    return r[1].data[0][1] != r[1].data[1][1]

def md5sums_as_rows(r):
    return Row(year = r[0][-7:-2], path = r[0], md5sum = r[1].data[0][1])

def write_csv(records, fieldnames):
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames )
    for record in records:
        writer.writerow(record.asDict())
    return [output.getvalue()]

def write_csv_no_match(records):
    return write_csv(records, ['key', 'path_1', 'md5sum_1', 'path_2', 'md5sum_2'])

def write_csv_match(records):
    return write_csv(records, ['year', 'key', 'path', 'md5sum'])

def write_csv_temp(records):
    return write_csv(records, ['path', 'key', 'md5sum'])



def md5sums_no_match_as_row(r):
    for item in r[1]:
        if 'tmp' in item[0]:
            md5sum_2 = item[1]
            path_2 = item[0]
        else:
            md5sum_1 = item[1]
            path_1 = item[0]
    return Row(key = r[0], path_1 = path_1, md5sum_1 = md5sum_1, path_2 = path_2, md5sum_2 = md5sum_2)

def md5sums_match_as_row(r):
    for item in r[1]:
        if 'tmp' in item[0]:
            continue
        else:
            md5sum = item[1]
            path = item[0]
    return Row(key = r[0], path = path, md5sum = md5sum, year = r[0][-7:-3])


def write_results(df1,  bucket, the_dir):
    #s3_delete(the_dir)
    path = "s3://{0}/{1}".format(bucket, the_dir)
    df1.coalesce(1).write.csv(path)
    #df1.write.csv(path)

def compare_rdds(sqlContext, rdd1, rdd2, bucket, s3_valid_dir):
    df1 = rdd1.toDF()
    df2 = rdd2.toDF()
    df1.registerTempTable("first")
    df1.registerTempTable("second")
    df_error = sqlContext.sql("""Select first.path as path1, first.md5sum as md5_sum1, second.path as path2, second.md5sum
            as md5_sum2 from first inner join second on first.key = second.key
             WHERE first.md5sum != second.md5sum
            """)
    write_results(df_error, bucket = bucket, the_dir = s3_valid_dir)

def main():
    sc  = SparkContext(appName = "validation {0}".format(datetime.datetime.now()))
    sqlContext = SQLContext(sc)
    bucket = 'paulhtremblay'
    start_year = 1991
    end_year = 1995


    rdd = sc.wholeTextFiles("s3://paulhtremblay/noaa_tmp/", 100000)
    print("count is {0}".format(rdd.count()))
    print("num partitions are {0}".format(rdd.getNumPartitions()))
    return



    rdd1 = make_rdd(sc, start_year, end_year)
    rdd2 = make_rdd(sc, start_year, end_year, validation = True)
    rdd1.cache()
    rdd2.cache()
    rdd_join = rdd1.union(rdd2)
    rdd_join.cache()
    rdd_by_keys = rdd_join.map(lambda x: (x.key, (x.path, x.md5sum)))\
            .groupByKey()
    rdd_by_keys.cache()
    rdd_no_match = rdd_by_keys\
            .filter(md5sums_no_match)

    if rdd_no_match.count() != 0:
        rdd_no_match\
            .map(md5sums_no_match_as_row)\
            .mapPartitions(write_csv_no_match)\
            .saveAsTextFile("s3://paulhtremblay/noaa/tmp/mdsums_no_match_{0}_{1}".format(start_year, end_year))

    rdd_by_keys\
            .filter(md5sums_match)\
            .map(md5sums_match_as_row)\
            .mapPartitions(write_csv_match)\
            .saveAsTextFile("s3://paulhtremblay/noaa/mdsums_s3_{0}_{1}".format(start_year, end_year))

if __name__ == '__main__':
    main()

