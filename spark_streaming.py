from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

import util.config as conf
from util.logger import Log4j

if __name__ == "__main__":
    conf = conf.Config()
    spark_conf = conf.spark_conf
    nc_conf = conf.nc_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"nc_conf: {nc_conf}")

    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = spark.sparkContext
    ssc = StreamingContext(sc, 20)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream(nc_conf.host, nc_conf.port)

    # Split each line into words
    words = lines.flatMap(lambda line: line.split(" "))

    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Q1: Print words with even counts
    def process_even_counts(time, rdd):
        even_counts = rdd.filter(lambda x: x[1] % 2 == 0)
        for record in even_counts.collect():
            print(record)

    wordCounts.foreachRDD(process_even_counts)

    # Q2: Print words with length > 1 and odd counts
    def process_odd_counts_long_words(time, rdd):
        odd_counts_long_words = rdd.filter(lambda x: len(x[0]) > 1 and x[1] % 2 != 0)
        for record in odd_counts_long_words.collect():
            print(record)

    wordCounts.foreachRDD(process_odd_counts_long_words)

    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
