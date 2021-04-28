from pyspark.sql import SparkSession
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession as ss
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import *
import time
import sys

spark = ss.builder.appName("SparkETL").getOrCreate()

customerdf = spark.read.option("inferSchema", "true").option(
    "header", "true").csv(sys.argv[1])

# Drop Nun rows
customerdf = customerdf.na.drop()

customerdf = customerdf.withColumn(
    "installs", regexp_replace(col('installs'), "[/,+]", ""))
customerdf = customerdf.withColumn(
    'installs', customerdf['installs'].cast("int"))

customerdf = customerdf.withColumn(
    "price", regexp_replace(col('price'), "[$]", ""))
customerdf = customerdf.withColumn('price', customerdf['price'].cast("float"))

customerdf = customerdf.withColumn(
    "app_size", regexp_replace(col('app_size'), "M", "e+6"))
customerdf = customerdf.withColumn(
    "app_size", regexp_replace(col('app_size'), "k", "e+3"))
customerdf = customerdf.withColumn("app_size", regexp_replace(
    col('app_size'), "Varies with device", "0"))
customerdf = customerdf.withColumn(
    'app_size', customerdf['app_size'].cast("float"))

customerdf = customerdf.withColumn(
    "last_updated", regexp_replace(col('last_updated'), "[/,+]", ""))
customerdf = customerdf.withColumn("last_updated", regexp_replace(col('last_updated'), "January", "1")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "February", "2")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "March", "3")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "April", "4")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "May", "5")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "June", "6")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "July", "7")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "August", "8")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "September", "9")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "October", "10")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "November", "11")) \
    .withColumn("last_updated", regexp_replace(col('last_updated'), "December", "12"))
customerdf = customerdf.withColumn("last_updated", F.from_unixtime(
    F.unix_timestamp('last_updated', 'M d yyyy')))
customerdf = customerdf.withColumn(
    "last_updated", F.to_timestamp('last_updated'))
customerdf = customerdf.withColumn(
    "download_date", F.to_timestamp("download_date"))

customerdf.write.format("parquet").mode("overwrite").save(sys.argv[2])
