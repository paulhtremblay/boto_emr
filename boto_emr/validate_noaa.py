from pyspark.sql import Row
from pyspark import SparkContext
import datetime
from  pyspark.sql import SQLContext
import datetime
import hashlib

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
        final.append(Row(md5sum = md5_sum, path = line[0]))
    return iter(final)

def compare_rdds(sqlContext, rdd1, rdd2):
    df1 = rdd1.toDF()
    df2 = rdd2.toDF()
    df1.registerTempTable("first")
    df1.registerTempTable("second")
    df_error_1 = sqlContext.sql("Select first.path as path1, first.md5sum as md5_sum1, second.path as path2, second.md5sum\
            as md5_sum2 from first left join second on first.path = second.path\
            where second.path is null or first.md5sum != second.md5sum")
    df_error_2 = sqlContext.sql("Select second.path as path2, second.md5sum as md5_sum2, first.path as path1, first.md5sum\
            as md5_sum2 from first left join second on first.path = second.path\
            where first.path is null")

def main():
    sc  = SparkContext(appName = "validation {0}".format(datetime.datetime.now()))
    sqlContext = SQLContext(sc)
    rdd1 = sc.wholeTextFiles("file:///home/paul/Downloads/tmp_chunks")\
            .mapPartitions(get_md5sum)
    rdd2 = sc.wholeTextFiles("file:///home/paul/Downloads/tmp_chunks2")\
            .mapPartitions(get_md5sum)
    compare_rdds(sqlContext, rdd1, rdd2)

if __name__ == '__main__':
    main()

