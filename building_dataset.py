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

# COMMAND ----------

season_length = pd.date_range(start="2022-08-14_00:00:00_UTC", end="2022-10-26_00:00:00_UTC", freq="1H")

# COMMAND ----------

# season_length = pd.date_range(start="2022-08-14 00:00:00", end="2022-10-26 00:00:00", freq="1H")  

# Unix time will be easier to work with 
season_length = list(range(1660460400, 1666771200, 3600)) # cutting it off early because I do not want to deal with null data at the moment. There is a way to clean it up though

column_list = ["id","dateTime","content","retweet_count", "like_count", "tweet_url"]
tweet_list = []

for i in range(len(season_length)-1):
  start = season_length[i]
  end = season_length[i+1]
  search_query = f'"House of the Dragon" OR to:HouseofDragon lang:en since_time:{start} until_time:{end}' # using since_time because it provides more precise query window timeframes 
  print(f"Beginning scrape hour: {start}")
  # print(search_query)
  for j, tweet in enumerate(sntwitter.TwitterSearchScraper(search_query).get_items()):
    if j < 50:
      tweet_list.append([tweet.id, tweet.date, tweet.content, tweet.retweetCount, tweet.likeCount, tweet.url]) # importing retweet and like counts for weighting in the future
    else:
      break
  print(f"Tweet count in this batch {j}")
  if not i%5:
    print(i)
    print(f"Oldest tweet retireved from {start}")
    print(tweet.date)
master_tweet_df = pd.DataFrame(tweet_list, columns=column_list)

# COMMAND ----------

master_tweet_df.tail(20)

# COMMAND ----------

# Save for later 

ep_airs = ["2022-08-21","2022-08-28", "2022-09-04", "2022-09-11", "2022-09-18", "2022-09-25", "2022-10-02", "2022-10-09", "2022-10-16", "2022-10-23"]
ep_ends = ["2022-08-26","2022-09-02", "2022-09-09", "2022-09-16", "2022-09-23", "2022-09-30", "2022-10-07", "2022-10-14", "2022-10-21", "2022-10-28"]
start = "2022-08-21"
end = "2022-08-26"

# season_length = pd.date_range(start="2022-08-14 00:00:00", end="2022-10-29 00:00:00", freq="1H")

column_list = ["id","dateTime","content","retweet_count", "like_count", "tweet_url"]
tweet_list = []
for i, start_date in enumerate(ep_airs):
  start = start_date
  end = ep_ends[i]
  search_query = f'"House of the Dragon" OR "HOTD" OR to:HouseofDragon lang:en since:{start} until:{end}'
  print(f"Beginning scrape for EP{i+1}")
  for i, tweet in enumerate(sntwitter.TwitterSearchScraper(search_query).get_items()):
    if i < 100:
      if not i%10:
        print(f"{i} tweets retrieved")
      tweet_list.append([tweet.id, tweet.date, tweet.content, tweet.retweetCount, tweet.likeCount, tweet.url]) # importing retweet and like counts for weighting in the future
    else:
      break
  print("Oldest tweet retireved")
  print(tweet.date)
m_tweet_df = pd.DataFrame(tweet_list, columns=column_list)

# COMMAND ----------

len(m_tweet_df)

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

# COMMAND ----------

old = [123]
old.append(tweet_list)

# COMMAND ----------

tweet_list

# COMMAND ----------

begin = 1660460400
n = 1660464000
n2 = 1660467600

# COMMAND ----------

n2 - begin

# COMMAND ----------

temp_list = list(range(1660460400, 1660467600, 3600))

# COMMAND ----------

temp_list

# COMMAND ----------


