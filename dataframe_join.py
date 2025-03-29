import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, col, desc, col, to_date, count
import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print(f"working_dir: {working_dir}")

    spark_conf = conf.get_spark_conf()

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    flight_time_df1 = spark.read.json("/data/dataframe-join/d1/")
    flight_time_df2 = spark.read.json("/data/dataframe-join/d2/")

    join_df = flight_time_df1.join(flight_time_df2, "id", "inner")
    join_df = join_df.withColumn("FL_DATE", to_date(col("FL_DATE"), "M/d/yyyy"))
    join_df.printSchema()
    join_df.show()

    # Q1: Cancelled flights to Atlanta, GA in 2000
    # Ordered by flight date descending.
    q1 = join_df.filter(
        (col("DEST_CITY_NAME") == "Atlanta, GA") &
        (col("CANCELLED") == 1) &
        (year(col("FL_DATE")) == 2000)
    ).select(
        col("id"),
        col("DEST"),
        col("DEST_CITY_NAME"),
        col("FL_DATE"),
        col("ORIGIN"),
        col("ORIGIN_CITY_NAME"),
        col("CANCELLED")
    ).orderBy(desc("FL_DATE"))
    q1.show()

    # Q2: Total cancelled flights by destination and year
    # Ordered by destination and year.
    q2 = join_df.filter(col("CANCELLED") == 1) \
        .groupBy(col("DEST"), year(col("FL_DATE")).alias("FL_YEAR")) \
        .agg(count("*").alias("NUM_CANCELLED_FLIGHT")) \
        .orderBy("DEST", "FL_YEAR")
    q2.show()

    spark.stop()
