import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from util.config import Config
from util.logger import Log4j

if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    nc_conf = conf.nc_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"nc_conf: {nc_conf}")

    socket_df = spark \
        .readStream \
        .format("socket") \
        .option("host", nc_conf.host) \
        .option("port", nc_conf.port) \
        .load()

    log.info(f"isStreaming: {socket_df.isStreaming}")

    socket_df.printSchema()

    word_df = socket_df.withColumn("word", f.explode(f.split("value", " ")))

    # Q1: Words with even counts
    even_counts_df = word_df \
        .groupBy("word") \
        .agg(f.count("*").alias("count")) \
        .filter(f.expr("count % 2 == 0")) \
        .withColumn("type", f.lit("EVEN"))

    # Q2: Words longer than 1 with odd counts
    odd_counts_df = word_df \
        .filter(f.length("word") > 1) \
        .groupBy("word") \
        .agg(f.count("*").alias("count")) \
        .filter(f.expr("count % 2 != 0")) \
        .withColumn("type", f.lit("ODD"))

    combined_df = even_counts_df.unionByName(odd_counts_df)
    combined_query = combined_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .trigger(processingTime="20 seconds") \
        .start()

    spark.streams.awaitAnyTermination()