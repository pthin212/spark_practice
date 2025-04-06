import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, MapType, BooleanType

from util.config import Config
from util.logger import Log4j

if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kaka_conf.items()}")

    df = spark.readStream \
        .format("kafka") \
        .options(**kaka_conf) \
        .load()

    df.printSchema()

    # Define the schema for selected JSON fields
    json_schema = StructType([
        StructField("_id", StringType(), True),
        StructField("time_stamp", LongType(), True),
        StructField("ip", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("resolution", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("api_version", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("local_time", StringType(), True),
        StructField("show_recommendation", BooleanType(), True),
        StructField("current_url", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("option", StringType(), True),
        StructField("id", StringType(), True)
    ])

    # Convert the value column from JSON string to StructType
    df_parsed = df.select(from_json(col("value").cast("string"), json_schema).alias("jsonData"))

    # Select the desired columns from the parsed JSON data
    df_selected = df_parsed.select(
        col("jsonData.id").alias("id"),
        col("jsonData.time_stamp").alias("time_stamp"),
        col("jsonData.ip").alias("ip"),
        col("jsonData.user_agent").alias("user_agent"),
        col("jsonData.resolution").alias("resolution"),
        col("jsonData.device_id").alias("device_id"),
        col("jsonData.api_version").alias("api_version"),
        col("jsonData.store_id").alias("store_id"),
        col("jsonData.local_time").alias("local_time"),
        col("jsonData.show_recommendation").alias("show_recommendation"),
        col("jsonData.current_url").alias("current_url"),
        col("jsonData.referrer_url").alias("referrer_url"),
        col("jsonData.email_address").alias("email_address"),
        col("jsonData.collection").alias("collection"),
        col("jsonData.product_id").alias("product_id"),
        col("jsonData.option").alias("option")
    )

    query = df_selected.writeStream \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()
