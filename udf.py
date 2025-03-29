import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, StringType

import gender_util.gender_util as gender_util
import util.config as conf
from util.logger import Log4j

def parse_employee_count(employee_range):
    # Parses employee range, handling "More than 1000" and hyphenated ranges
    if employee_range is None:
        return -1
    try:
        if employee_range == "More than 1000":
            return 1001
        elif "-" in employee_range:
            parts = re.split(r"\s*-\s*", employee_range)
            if len(parts) == 2:
                lower = int(parts[0])
                upper = int(parts[1])
                return (lower + upper) // 2
            else:
                return -1
        elif employee_range.isdigit():
            return int(employee_range)
        else:
            return -1

    except (ValueError, IndexError):
        return -1


if __name__ == '__main__':
    working_dir = os.getcwd()
    print(f"working_dir: {working_dir}")

    spark_conf = conf.get_spark_conf()
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    log = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/data/udf/survey.csv")
    survey_df.printSchema()
    survey_df.show()

    # Q: Display records with 500 or more employees.
    parse_employee_count_udf = udf(parse_employee_count, IntegerType())
    spark.udf.register("parse_gender_udf", gender_util.parse_gender, StringType())

    survey_df = survey_df \
        .withColumn("employee_count", parse_employee_count_udf(col("no_employees"))) \
        .withColumn("Gender", gender_util.parse_gender_udf(col("Gender"))) \
        .filter(col("employee_count") >= 500)

    survey_df.select("Age", "Gender", "Country", "state", "no_employees").show()
    spark.stop()