import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

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

    summary_df = spark.read.parquet("/data/window-function/summary.parquet")
    summary_df.printSchema()
    summary_df.show()

    # Q1: Rank by InvoiceValue within each country
    window_spec_country = Window.partitionBy("Country").orderBy(f.desc("InvoiceValue"))

    ranked_df = summary_df.withColumn("rank", f.rank().over(window_spec_country))
    ranked_df = ranked_df.select("Country", "WeekNumber", "NumInvoices", "TotalQuantity", "InvoiceValue", "rank")

    ranked_df.orderBy("Country", "rank").show()

    # Q2: Calculate Percentage Growth and Accumulative Value
    window_spec_country_week = Window.partitionBy("Country").orderBy("WeekNumber")

    result_df = summary_df.withColumn("LagInvoiceValue", f.lag("InvoiceValue", 1, 0).over(window_spec_country_week)) \
        .withColumn("PercentGrowth",
            f.when(f.col("LagInvoiceValue") != 0,
                   f.round((f.col("InvoiceValue") - f.col("LagInvoiceValue")) * 100 / f.col("LagInvoiceValue"), 2)).otherwise(0.0)) \
        .withColumn("AccumulateValue",
            f.round(f.sum("InvoiceValue").over(window_spec_country_week), 2))  # Round to 2 decimal places

    result_df = result_df.select("Country", "WeekNumber", "NumInvoices", "TotalQuantity", "InvoiceValue",
                                 "PercentGrowth", "AccumulateValue")  # Select specified columns

    result_df.orderBy("Country", "WeekNumber").show()

    spark.stop()