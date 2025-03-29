import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, lower, count, when

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print("working_dir: " + working_dir)
    spark_conf = conf.get_spark_conf()
    spark = SparkSession \
        .builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/data/dataframe-api/survey.csv")

    log.info("survey_df schema: ")
    survey_df.printSchema()

    # Q1: Filters for males under 40
    # Groups by country
    # Counts, and sorts by count then country
    r1 = survey_df \
        .filter((col("Age") < 40) &
                (lower(col("Gender")).isin("male", "m"))) \
        .groupBy("Country") \
        .agg(count("*").alias("male_under_40_count")) \
        .orderBy(col("male_under_40_count"), col("Country"))

    r1.show()

    # Q2: Counts males and females per country
    # Sorted by country name
    r2 = survey_df \
        .groupBy("Country") \
        .agg(
        count(when(lower(col("Gender")).isin("male", "m", "man"), 1)).alias("male_count"),
        count(when(lower(col("Gender")).isin("female", "w", "woman"), 1)).alias("female_count")
    ) \
        .orderBy("Country")

    r2.show()

    spark.stop()
