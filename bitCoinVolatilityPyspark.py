#### Import necesary libraries ####
import json 						#To work with jsons
import requests 					#To get http requests
#import pyspark 						#To use pyspark
#import findspark          #To find the current spark version locally
import pyspark.sql.functions as f 	#Access to spark functions (col, to_date, stddev, sum)


#### Create Spark Session ####
#findspark.init(env_variables['Spark_env'])
#conf = pyspark.SparkConf().setAppName('BTCoinVolatility').setMaster('local')
#sc = pyspark.SparkContext(conf=conf)
#sc.setLogLevel("ERROR")
#spark = SparkSession(sc)

#### Data Base Credentials ####
hostname =  ""
database =  ""
username =  ""
password =  ""

#### Establish Connection properties for Database ####
jdbc_url = 'jdbc:{0}://{1}:{2};database={3}'.format( 'sqlserver', hostname, 1433, database )
connection_properties = {
  "username" : username,
  "password" : password,
  "driver" : 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}

#### Get Data ####
#Get data from URL and load it to a json object
jsonString = requests.get("http://cf-code-challenge-40ziu6ep60m9.s3-website.eu-central-1.amazonaws.com/ohlcv-btc-usd-history-6min-2020.json").content
jsonObject = json.loads(jsonString)

#### Create Dataframe from json collected ####
BTCoin_df = spark.read.json(sc.parallelize(jsonObject))

#### Transformations ####
#Declare new dataframe to work with
BTCoin_Transformation = BTCoin_df

#- Clean Start Date -#
#Create new column ('StartDate') without time for aggregation purposes
BTCoin_Transformation = BTCoin_Transformation.withColumn('StartDate',f.to_date(f.col('time_period_start')))

#- Price Avg -#
#Create new column ('PriceAvg') with an average of the prices in the data for a better analysis
BTCoin_Transformation = BTCoin_Transformation.withColumn('PriceAvg', (f.col('price_close')+f.col('price_high')+f.col('price_low')+f.col('price_open'))/4)

#- Aggregations -#
#Aggregations for standard deviation and other sums can add value to the data
BTCoin_Agg = BTCoin_Transformation.groupBy('StartDate').agg(f.stddev('PriceAvg').alias('PriceStd'),f.sum('trades_count').alias('DailyTrades'),f.sum('volume_traded').alias('DailyVolume'))

#### Insert Raw data in DB ####
BTCoin_df.write.mode("overwrite").option("truncate", True).jdbc(url = jdbc_url, table = "[dbo].[BTCPeriodicRawData]", properties = connection_properties)

#### Insert data in DB from the previous dataframe (BTCoin_Agg) ####
BTCoin_Agg.write.mode("overwrite").option("truncate", True).jdbc(url = jdbc_url, table = "[dbo].[BTCoinDailyData]", properties = connection_properties)

#### Close Spark Context ####
#sc.stop()S