# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ETL for House of the Dragon dataset 
# MAGIC 1. Read Data from DBFS
# MAGIC * Using option("multiline") is necessary
# MAGIC * Make sure to escape quotation marks
# MAGIC * Partition DataFrame
# MAGIC 2. Convert Spark DataFrame to parquet table
# MAGIC 3. Convert Spark DataFrame to Delta table 
# MAGIC * Save table as delta files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data and create partitions

# COMMAND ----------

# MAGIC %pip install emoji

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, regexp_replace, to_date, pandas_udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover
import pandas as pd
import emoji

# COMMAND ----------

spark = SparkSession\
  .builder\
  .config("spark.pyspark.python", "python3")\
  .appName("HOTD")\
  .getOrCreate()

# COMMAND ----------

# Read data from csv into Spark DF

no_limits = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .option("multiline", "true")\
  .option("escape", "\"")\
  .load(r"/home/kam.look@databricks.com/nolimits2_twitter_dataset.csv")
# Why is it split into two jobs? Even after reading the guide I was a bit confused 

# COMMAND ----------

# Create date partition column
no_limits = no_limits.withColumn("p_date",
                    to_date("dateTime","yyyy-MM-dd"))
no_limits.select("dateTime", "p_date").show()

# COMMAND ----------

# convert emojis into string use pandas udf not python, in order to leverage vectorized  udf 

def emoji_to_str_func(s: pd.Series) -> pd.Series:
  return s.map(lambda x: emoji.demojize(x))
  
emoji_to_str = pandas_udf(emoji_to_str_func, returnType=StringType())

no_limits = no_limits.withColumn("str_content",
                                emoji_to_str(col("content")))

# COMMAND ----------

no_limits.select("content", "str_content").show(truncate = False)

# COMMAND ----------

# Cleaning DF while in memory

# hashtag_regex = "\B\#\w+" # identify all hashtags, BUT removing all hastags may be bad.e.g. #WorstEpYet 
url_regex = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)|[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)" # identify URLs
puncuation_regex = r"[^\w\s]"
whitespace_regex = r"\s{2,}"
colon_regex = r":+"
https_regex = "https"
# regex_str = hashtag_regex + "|" + url_regex
regex_str = puncuation_regex + "|" + url_regex + "|" + https_regex

# COMMAND ----------

# replace content column with new, cleaner content 
no_limits = no_limits.withColumn("clean_content",
                                regexp_replace("str_content", colon_regex, " ")) # breaks url parser, since we are removing colons 
no_limits = no_limits.withColumn("clean_content",
                                regexp_replace("clean_content", whitespace_regex, " "))
no_limits = no_limits.withColumn("clean_content",
                                regexp_replace("clean_content", regex_str, ""))

# COMMAND ----------

no_limits.select("content", "clean_content").show(truncate=False)

# COMMAND ----------

# tokenize 
tokenizer_hotd = Tokenizer(inputCol="clean_content", outputCol="tokens")
no_limits = tokenizer_hotd.transform(no_limits)

# COMMAND ----------

no_limits.select("tokens").show(truncate=False)

# COMMAND ----------

# remove stop words
eng_stop_words = StopWordsRemover.loadDefaultStopWords("english")
stops = StopWordsRemover()\
  .setStopWords(eng_stop_words)\
  .setInputCol("tokens")\
  .setOutputCol("clean_tokens")
no_limits = stops.transform(no_limits)

# COMMAND ----------

no_limits.select("tokens","clean_tokens").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write DF to Parq and Delta, create database and tables

# COMMAND ----------

# If need to remove old files 

dbutils.fs.rm('/home/kam.look@databricks.com/parq_files', recurse=True)
dbutils.fs.rm('/home/kam.look@databricks.com/delta_files', recurse=True)
# spark.sql("DROP TABLE kam_look_hotd.parq_table")
# spark.sql("DROP TABLE kam_look_hotd.delta_table")

# COMMAND ----------

# Save parquet files 
no_limits.write.format("parquet").partitionBy("p_date")\
  .option("mode", "overwrite")\
  .option("path", r"/home/kam.look@databricks.com/parq_files")\
  .save()

# COMMAND ----------

# Create DELTA files here
no_limits.write.format("delta").partitionBy("p_date")\
  .option("mode", "overwrite")\
  .option("path", r"/home/kam.look@databricks.com/delta_files")\
  .save()

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS kam_look_hotd;

# COMMAND ----------

spark.sql("DROP TABLE kam_look_hotd.parq_table") # Simply replacing the data and running MSCK repair does not work. Must drop and re-add data
spark.sql("DROP TABLE kam_look_hotd.delta_table") # normally, we REPLACE delta tables, not drop and re-add

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE kam_look_hotd.parq_table 
# MAGIC USING PARQUET
# MAGIC LOCATION '/home/kam.look@databricks.com/parq_files/';
# MAGIC 
# MAGIC CREATE TABLE kam_look_hotd.delta_table
# MAGIC USING DELTA
# MAGIC LOCATION '/home/kam.look@databricks.com/delta_files';

# COMMAND ----------

spark.sql("MSCK REPAIR TABLE kam_look_hotd.parq_table")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Performance of  parquet table

# COMMAND ----------

# Save as parquet files and later read as parquet table b

# COMMAND ----------

dbutils.fs.ls("/home/kam.look@databricks.com/parq_files")

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/kam_look_hotd.db")

# COMMAND ----------

# why can i not see the data in my parq table? 
spark.sql("""
SELECT clean_tokens, p_date
FROM kam_look_hotd.parq_table""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance of Delta table

# COMMAND ----------

spark.sql("""
SELECT clean_tokens, p_date
FROM kam_look_hotd.delta_table""").show(truncate=False)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Test area/ Scratch paper

# COMMAND ----------

small_df = spark.read.format("csv")\
  .option("header", "true")\
  .option("multiline", "true")\ 
  .load(r"/home/kam.look@databricks.com/small_twitter_dataset.csv")

# COMMAND ----------

dbutils.fs.cp('dbfs:/home/kam.look@databricks.com/small_twitter_dataset.csv','dbfs:/FileStore/')

# COMMAND ----------

print(f"Size of Spark Dataframe: {len(hotd_df.columns)}, {hotd_df.count()}")

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC cd '/dbfs/home/kam.look@databricks.com/delta_files'
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls('/home/kam.look@databricks.com/delta_files')

# COMMAND ----------


