import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, to_date, year, countDistinct

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print(f"working_dir: {working_dir}")

    spark_conf = conf.get_spark_conf()

    spark = SparkSession \
        .builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    invoice_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/data/agg-group/invoices.csv")

    log.info("invoice_df schema:")
    invoice_df.printSchema()

    # Q1: Aggregate invoice data by country and year, calculating total quantity, amount, and invoice count.
    # Sort results by country and year.
    invoice_df = invoice_df.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .withColumn("Year", year(col("InvoiceDate")))

    q1 = invoice_df \
        .groupBy("Country", "Year") \
        .agg(
            f.countDistinct(col("InvoiceNo")).alias("num_invoices"),
            f.round(f.sum(col("Quantity") * col("UnitPrice")), 2).alias("invoice_value")
        ) \
        .orderBy("Country","Year")
    q1.show()

    # Q2: Find top 10 customers with the highest total purchase amount in 2010.
    # Sort by total amount descending, then customer ID ascending.
    invoice_2010_df = invoice_df.filter((col("Year") == 2010) & (col("CustomerID").isNotNull()))
    invoice_2010_df.printSchema()
    customer_total_df = invoice_2010_df \
        .groupBy("CustomerID") \
        .agg(
            f.round(f.sum(col("Quantity") * col("UnitPrice")), 2).alias("TotalAmount")
        )

    q2 = customer_total_df.orderBy(f.desc("TotalAmount"), f.asc("CustomerID")) \
        .limit(10)
    q2.show()

    spark.stop()
