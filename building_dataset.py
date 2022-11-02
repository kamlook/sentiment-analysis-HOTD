# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Notebook for scraping Twitter for tweets about "House of the Dragon"

# COMMAND ----------

# MAGIC %pip install snscrape
# MAGIC 
# MAGIC # May need to install other dependencies with %pip in order for package to work 

# COMMAND ----------

import snscrape.modules.twitter as sntwitter
import pandas as pd
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Build dataset spanning across Season 1 airtime
# MAGIC 
# MAGIC ### Data retrieved hourly across all air time, capped at 50 tweets per hour. Takes 35 minutes to run and get 86,000 tweets total. If needed I can increase this.

# COMMAND ----------

# season_length = pd.date_range(start="2022-08-14 00:00:00", end="2022-10-26 00:00:00", freq="1H")  

# Unix time will be easier to work with 
season_length = list(range(1660460400, 1666839600, 3600)) # cutting it off early because I do not want to deal with null data at the moment. There is a way to clean it up though

column_list = ["id","dateTime","content","retweet_count", "like_count", "tweet_url"]
tweet_list = []

for i in range(len(season_length)-1):
  start = season_length[i]
  end = season_length[i+1]
  search_query = f'"House of the Dragon" OR to:HouseofDragon lang:en since_time:{start} until_time:{end}' # using since_time because it provides more precise query window timeframes 
  print(f"Beginning scrape hour: {start}")
  # print(search_query)
  for j, tweet in enumerate(sntwitter.TwitterSearchScraper(search_query).get_items()):
    if j < 1500:
      tweet_list.append([tweet.id, tweet.date, tweet.content, tweet.retweetCount, tweet.likeCount, tweet.url]) # importing retweet and like counts for weighting in the future
    else:
      break
  print(f"Tweet count in this batch {j}")
  if not i%10:
    print(i)
    print(f"Oldest tweet retireved from {start}")
    print(tweet.date)
master_tweet_df = pd.DataFrame(tweet_list, columns=column_list)

# COMMAND ----------

# If range method above does not work, just download EVERYTHING 
len(master_tweet_df)

# COMMAND ----------

# Save this dataframe as a CSV to DBFS
# Small: 86k records, 30 min 
# Medium: 381k records, 2.17 hours
# Large: 
master_tweet_df.to_csv('/dbfs/home/kam.look@databricks.com/medium_twitter_dataset.csv')

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -l '/dbfs/home/'

# COMMAND ----------

# Save for later 
# season_length = pd.date_range(start="2022-08-14 00:00:00", end="2022-10-29 00:00:00", freq="1H")

column_list = ["id","dateTime","content","retweet_count", "like_count", "tweet_url"]
tweet_list = []

search_query = f'"House of the Dragon" OR #HouseOfTheDragon OR #HouseOfTheDragonHBO OR #HOTD OR to:HouseofDragon lang:en since:2022-08-14 until:2022-10-30'
for i, tweet in enumerate(sntwitter.TwitterSearchScraper(search_query).get_items()):
  if not i%10000:
    print(f"{i} tweets retrieved")
    print(f"Current tweet date: {tweet.date}")
  tweet_list.append([tweet.id, tweet.date, tweet.content, tweet.retweetCount, tweet.likeCount, tweet.url]) # importing retweet and like counts for weighting in the future
m_tweet_df = pd.DataFrame(tweet_list, columns=column_list)
print(f"Size of DF: {len(m_tweet_df)}")
m_tweet_df.to_csv('/dbfs/home/kam.look@databricks.com/nolimits2_twitter_dataset.csv')

# COMMAND ----------

json_tweet_df.head()

# COMMAND ----------

m_tweet_df.tail(200)

# COMMAND ----------

tweet_list = []
test_query = f'"House of the Dragon" OR "HOTD" OR to:HouseofDragon lang:en since:2022-08-14_00:00:00_UTC until:2022-08-14_01:00:00_UTC'
test_query = f'"House of the Dragon" OR "HOTD" OR to:HouseofDragon lang:en since_time:1661842800 until_time:1661878800'
for i, tweet in enumerate(sntwitter.TwitterSearchScraper(test_query).get_items()):
  if i > 50:
    break
  tweet_list.append([tweet.id, tweet.date, tweet.content, tweet.retweetCount, tweet.likeCount, tweet.url])
