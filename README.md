# BitCoinVolatility
This is a challenge which purpose is to transform and transfer the data gathered by http request into a SQL Database.

## The Thought Process ##
After reviewing the document I decided to solve the challenge using python and pyspark as these are the tools that I can handle the best and I wanted to state my idea of how I would work out the solution using them.
The second thing I did was to review the data to understand its structure and figure out which was the best approach to the desired solution, also which would be other properties that might be useful information.
I decided to use the `time_period_start` property as the main column as I could take out the time, and I considered it perfect for the daily aggregations.
For the **Standard Deviation** I knew I had to calculate it based on the price, given 4 different prices I decided it would be best to use them to get an average for each time period, creating the `PriceAvg` column to which I applied the standard deviation later.
The other properties I thought would be useful where `trades_count` and `volume_traded`. I applied sum to each of them to have a daily aggregation for this data.
So the result is a table that contains 4 columns: `StartDate`, `PriceStd`, `DailyTrades` and `DailyVolume` which I believe has not just the insight expected but also 2 more columns that help understand the amount of trades and volumes that happened on a daily basis. This might help to a future analysis on the growth or withering of the Bit Coin.

## Instructions ##
There are several ways to run this code. I used azure databricks which is cloud based, but also included some lines of code that help when this code is runned in a local environment like using jupyter and anaconda.

In case you try to run it in a local environment you will need to install anaconda, jupyter and pyspark. After that the code can be runned in a jupyter notebook.

## The Code ##
1. On the first block from line 1 to 5:
These are imports that would be needed for the proper running of the code.
- **json**: Helps to work with json structures.
- **requests**: Helps with the handling of http requests.
- **pyspark<sup>1</sup>**: Core library in which the spark function work 
- **pyspark.sqk.functions<sup>1</sup>**: Stored as `f` to easy access to the methods on this library. The used methods where `col` to access certain columns in a dataframe, `to_date` to parse datetime data to date, `stddev` for the **Standard Deviation** aggregation and `sum` for the sum aggregations.
```
#### Import necesary libraries ####
import json 						
import requests 					
import pyspark<sup>1</sup> 						
import pyspark.sql.functions as f 
```
2. The next block from lines 8 and 9 are for the initialization of the **SparkSession** which helps establish the spark running environment.
```
#### Create Spark Session ####
spark = SparkSession.builder().master("local[1]").appName("BTCoinVolatility").getOrCreate()<sup>1</sup>
```
3. The code block from line 11 to 23 are to establish the connection with the desired database for later storage.
It is written earlier in the code for order purposes. This labels will be later used in the writing process.
```
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
```
4. The next code block from line 25 to 30 is where the data is gathered and processed to become a dataframe.
The count method can be applied to the dataframe to see how much data it contains ().
```
#### Get Data ####
#Get data from URL and load it to a json object
jsonString = requests.get("http://cf-code-challenge-40ziu6ep60m9.s3-website.eu-central-1.amazonaws.com/ohlcv-btc-usd-history-6min-2020.json").content
jsonObject = json.loads(jsonString)

#### Create Dataframe from json collected ####
BTCoin_df = spark.read.json(sc.parallelize(jsonObject))
```
5. This next code block from line 33 to 47 contains the transformation process for the data.
First I create a new label called **BTCoin_Transformation** and assign the original dataframe **BTCoin_df** to it to handle the transformations in a cleaner way.
After this I used the spark method `withColumn` to create 2 columns: `StartDate` which contains the parsed date from `time_period_start` and `PriceAvg` where I took the average from the prices in each time period.
Finaly I used the spark methods `groupBy` and `agg` to aggregate the data for the standard deviation and sums generating a new dataframe assigned to the label **BTCoin_Agg**.
This dataframe contains the 4 columns and data that is going to be stored in the database.
```
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
```
6. Finaly in this last block of code the data inside the dataframe **BTCoin_Agg** is stored inside a desired table (I called this table BTCoinDailyData) in the database configured earlier in the code.
```
#### Insert data in DB from the previous dataframe (BTCoin_Agg) ####
BTCoin_Agg.write.mode("overwrite").option("truncate", True).jdbc(url = jdbc_url, table = "[dbo].[BTCoinDailyData]", properties = connection_properties)
```

##Notes##
*1: Code that needs to be added if runned in a local environment or one that needs to create a spark context environment
