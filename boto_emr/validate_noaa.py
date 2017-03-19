from pyspark.sql import Row
from pyspark import SparkContext
import datetime
from  pyspark.sql import SQLContext
import datetime
import hashlib
import os
import boto3

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

def make_rdd(sc, start_year, end_year, validation = ''):
    for counter, year in enumerate(range(start_year, end_year + 1)):
        rdd_temp = sc.wholeTextFiles("s3://paulhtremblay/noaa{0}/{1}/".format(validation, year))\
            .mapPartitions(get_md5sum)
        if counter == 0:
            rdd = rdd_temp
        else:
            rdd.union(rdd_temp)
    return rdd

def write_results(df1,  bucket, the_dir):
    s3_delete(the_dir)
    path = "s3://{0}/{1}".format(bucket, the_dir)
    df1.coalesce(1).write.csv(path)

def compare_rdds(sqlContext, rdd1, rdd2, bucket, s3_valid_dir):
    df1 = rdd1.toDF()
    df2 = rdd2.toDF()
    df1.registerTempTable("first")
    df1.registerTempTable("second")
    df_error = sqlContext.sql("Select first.path as path1, first.md5sum as md5_sum1, second.path as path2, second.md5sum\
            as md5_sum2 from first full outer join second on first.key = second.key\
            where second.path is null or first.md5sum != second.md5sum")
    write_results(df_error, bucket = bucket, the_dir = s3_valid_dir)

def main():
    sc  = SparkContext(appName = "validation {0}".format(datetime.datetime.now()))
    sqlContext = SQLContext(sc)
    bucket = 'paulhtremblay'
    start_year = 1901
    end_year = 1903
    rdd1 = make_rdd(sc, start_year, end_year)
    rdd2 = make_rdd(sc, start_year, end_year, validation = '_validation')
    s3_valid_dir = "noaa_validation_results"
    compare_rdds(sqlContext, rdd1, rdd2, bucket, s3_valid_dir)

if __name__ == '__main__':
    main()

