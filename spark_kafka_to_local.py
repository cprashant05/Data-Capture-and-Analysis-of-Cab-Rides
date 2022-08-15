# Importing the modules

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window


# Initializing Spark Session

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("CapstoneProject") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Creating Dataframe from Kafka Data

clickstream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("subscribe", "de-capstone3") \
    .option("startingOffsets", "earliest") \
    .load()

clickstream_df.printSchema()

# Transform dataframe by dropping few columns and changing value column data type

clickstream_df = clickstream_df \
    .withColumn('value_str', clickstream_df['value'].cast('string').alias('key_str')).drop('value') \
    .drop('key','topic','partition','offset','timestamp','timestampType')

# writing the dataframe to local file directory and keep it running until terminated

clickstream_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("truncate","false") \
    .option("path", "clickstream_data") \
    .option("checkpointLocation","clickstream_checkpoint") \
    .start() \
    .awaitTermination()