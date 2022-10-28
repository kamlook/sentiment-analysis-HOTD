# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ETL for House of the Dragon dataset 
# MAGIC 1. Read Data from DBFS
# MAGIC * Using option("multiline") is necessary
# MAGIC * Make sure to escape quotation marks 
# MAGIC 2. Convert Spark DataFrame to parquet table
# MAGIC 3. Convert Spark DataFrame to Delta table 

# COMMAND ----------

from pyspark.sql import SparkSession

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
  .load(r"/home/kam.look@databricks.com/nolimits_twitter_dataset.csv")
# Why is it split into two jobs? Even after reading the guide I was a bit confused 

# COMMAND ----------

# Create Parquet table 
parq_table = "HOTD Parquet Table"

no_limits.createOrReplaceTempView(parq_table)
df.write.format("parquet").saveAsTable("Permanent HOTD Parquet Table")

# COMMAND ----------

# Verify quality of the data
no_limits.where("tweet_url NOT LIKE '%twitter%'").show()

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
