try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
#    from operator import add
except Exception as e:
    print(e)

## http://www.hongyusu.com/imt/technology/spark-via-python-basic-setup-count-lines-and-word-counts.html
def get_counts():
    conf = SparkConf().setAppName('words count')
    conf = conf.setMaster('spark://master:7077')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # core part of the script
    lines = sc.textFile("README.md")
    words = lines.flatMap(lambda x: x.split(' '))
    pairs = words.map(lambda x: (x,1))
    count = pairs.reduceByKey(lambda x,y: x+y)

    # output results
#    for x in count.collect():
#        print(x)

    ## https://stackoverflow.com/questions/40069264/how-can-i-save-an-rdd-into-hdfs-and-later-read-it-back
    hdfs = "hdfs://hadoop:8020/"
    ## Writing file in CSV format
    count.toDF().write.format("com.databricks.spark.csv").mode("overwrite").save(hdfs + "user/me/count.csv")

    # End the Spark Context
    sc.stop()

if __name__ == "__main__":
    get_counts()
