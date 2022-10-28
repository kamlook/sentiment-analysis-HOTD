# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ETL for House of the Dragon dataset 
# MAGIC 1. Read Data from DBFS
# MAGIC * Using option("multiline") is necessary
# MAGIC * Make sure to escape quotation marks 
# MAGIC 2. Convert Spark DataFrame to parquet table
# MAGIC 3. Convert Spark DataFrame to Delta table 
# MAGIC * Learn how to create delta table WITHOUT converting from parquet

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, regexp_replace

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

# Verify quality of the data
no_limits.where("tweet_url NOT LIKE '%twitter%'").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Performance of DF while in memory

# COMMAND ----------

#Test speed of querying on a spark dataframe 
print(f"Count: {no_limits.count()}")

# COMMAND ----------

# Clean "content" column using RegEx
# 1. remove all Hashtags
# 2. remove all links and images
# 3. Keep emojis (for now)
hashtag_regex = "\B\#\w+" # identify all hashtags
url_regex = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)|[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)" # identify URLs

regex_str = hashtag_regex + "|" + url_regex

# COMMAND ----------

no_limits.select(
regexp_replace("content", regex_str, "").alias("clean_content"),
"content").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Performance of  parquet table

# COMMAND ----------

# Create Parquet table 
no_limits.write.format("parquet").saveAsTable("Permanent_HOTD_Parquet_Table")

# COMMAND ----------

spark.sql("""
SELECT COUNT(_c0)
FROM Permanent_HOTD_Parquet_Table""").show()

# COMMAND ----------

spark.sql("""
SELECT content
FROM Permanent_HOTD_Parquet_Table
WHERE content LIKE '%https://%'
LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance of Delta table

# COMMAND ----------

# Create DELTA table here

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
