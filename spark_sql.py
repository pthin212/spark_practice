import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col

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

    surveyDf = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/data/spark-sql/survey.csv")

    log.info("survey df schema: ")
    surveyDf.printSchema()

    # Q1: Filters for males under 40
    # Groups by country
    # Counts, and sorts by count then country
    surveyDf.createOrReplaceTempView("survey")
    r1 = spark.sql("""
        SELECT country, COUNT(*) AS male_under_40_count
        FROM survey
        WHERE age < 40
            AND (LOWER(gender) = 'male' OR LOWER(gender) = 'm')
        GROUP BY country
        ORDER BY male_under_40_count, country
    """)

    r1.show()

    # Q2: Counts males and females per country
    # Sorted by country name
    r2 = spark.sql("""
        SELECT country,
            COUNT(CASE WHEN LOWER(gender) in ('male', 'm', 'man') THEN 1 END) AS male_count,
            COUNT(CASE WHEN LOWER(gender) in ('female', 'w', 'woman') THEN 1 END) AS female_count
        FROM survey
        GROUP BY country
        ORDER BY country
    """)

    r2.show()

    spark.stop()
