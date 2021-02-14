from pyspark.sql import SparkSession
from pyspark.sql import window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpyas np
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("Wrangling Data").getOrCreate()

path = "hdfs://ec2-34-218-86-174.us-west-2.compute.amazonaws.com:9000/sparkify/sparkify_log_small.jsom"
user_log = spark.read.json(path)

user_log.take(5)

user_log.printSchema()

user_log.describe('artist').show()
user_log.describe('asessionId').show()

user_log.count()

user_log.select("page").dropDuplicates().sort("page").show()

user_log.select(["userTd", "firstname", "page", "song"]).where(user_log.userId == '1046').collect()

get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0), hour)
user_log = user_log.withColumn("hour", get_hour(user_log.ts))
user_log.head()

songs_in_hour = user_log.filter(user_log.page == "NextSong").groupby(user_log.hour).count().orderby(user_log.hour.cast("float"))
songs_in_hour.show()

songs_in_hour_pd = songs_in_hour.toPandas()
plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24)
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlable("Hour")
plt.ylabel("Songs Played")

user_log_valid = user_log.dropna(how = "any", subset= ["userId", "sessionId"])
user_log_valid.count()

user_log.select("userId").dropDuplicates().sort("userId").show()

user_log_valid = user_log_valid.filter(user_log_valid["userId"] != "")
user_log_valid.count()

user_log_valid.filter("page = 'Submit Downgrade'").show()

user_log.select(["userId", "firstname", "page", "level", "song"]).where(user_log.userId == '1138').collect()

flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

user_log_valid = user_log_valid.withColumn("downgrade", flag_downgrade_event("page"))
user_log_valid.head()

windowval = window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(window.unboundedPreceding, 0)
user_log_valid = user_log_valid.withColumn("phase", Fsum("downgrade").over(windowval))
user_log_valid.select([""]).where(user_log.userId == "1138").sort("ts").collect()