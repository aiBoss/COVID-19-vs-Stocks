# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # The Downfall of the Bulls!
# MAGIC ## Is it the right time to invest? Let's find out!
# MAGIC 
# MAGIC Author: Mourya Karan Reddy Baddam  
# MAGIC Id: 5564234  
# MAGIC Course: CSCI 5751 - Big Data Engineering and Architechture  
# MAGIC University of Minnesota
# MAGIC 
# MAGIC *Disclaimer:* **This is not a financial advice.** This is just an analysis of current situations. Any suggestion is the sole opinion of the author and are only valid until the trends mentioned in the analysis prevail.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fetching Data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The stock info and stock price data was downloaded from Yahoo! finance using a webscraping package 'yfinance'. The package had some issues and I edited the source code to include error handling and used multiprocessing to download data faster. I downloaded the data for the period from 01-02-2018 to 04-30-2020. I pushed the downloaded data to GitHub public repository. The data and the code for downloading can be found at https://github.com/aiBoss/Spark-Data
# MAGIC 
# MAGIC Covid - us_confirmed dataset was downloaded from GitHub open datasets at https://github.com/datasets/covid-19/tree/master/data

# COMMAND ----------

# DBTITLE 1,Downloading data from GitHub public repos and creating data folders.
# MAGIC %sh
# MAGIC rm -rf stock_and_covid_data
# MAGIC #Getting stock data from my GitHub Public repo
# MAGIC wget https://raw.githubusercontent.com/aiBoss/Spark-Data/master/raw_data.csv
# MAGIC wget https://raw.githubusercontent.com/aiBoss/Spark-Data/master/stock_info.csv
# MAGIC 
# MAGIC #Getting covid-19 data from GitHub public datasets repo
# MAGIC wget https://raw.githubusercontent.com/datasets/covid-19/master/data/us_confirmed.csv
# MAGIC 
# MAGIC #Moving the csv files to project folder
# MAGIC mkdir stock_and_covid_data
# MAGIC mv *.csv stock_and_covid_data/

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Processing

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC S&P 500 Index is a cap-weighted Index of the 505 major stocks in the US. It comprises of stocks which are divided into 11 broad business categories.
# MAGIC 
# MAGIC Defining Stock List, Stock Categories and Category Dictionary to store the List of stocks in each category.
# MAGIC 
# MAGIC Source: Wikipedia
# MAGIC 
# MAGIC Entry method: Manual

# COMMAND ----------

# DBTITLE 1,Defining Stock List, Stock Categories and Category Dictionary.
ticker = [	'MMM','ABT','ABBV','ABMD','ACN','ATVI','ADBE','AMD','AAP','AES','AFL','A','APD','AKAM','ALK','ALB','ARE','ALXN','ALGN','ALLE',
			'AGN','ADS','LNT','ALL','GOOGL','GOOG','MO','AMZN','AMCR','AEE','AAL','AEP','AXP','AIG','AMT','AWK','AMP','ABC','AME',
			'AMGN','APH','ADI','ANSS','ANTM','AON','AOS','APA','AIV','AAPL','AMAT','APTV','ADM','ANET','AJG','AIZ','T','ATO','ADSK',
			'ADP','AZO','AVB','AVY','BKR','BLL','BAC','BK','BAX','BDX','BBY','BIIB','BLK','BA','BKNG','BWA','BXP','BSX','BMY','AVGO',
			'BR','CHRW','COG','CDNS','CPB','COF','CPRI','CAH','KMX','CCL','CARR','CAT','CBOE','CBRE','CDW','CE','CNC','CNP','CTL',
			'CERN','CF','SCHW','CHTR','CVX','CMG','CB','CHD','CI','CINF','CTAS','CSCO','C','CFG','CTXS','CLX','CME','CMS','KO','CTSH',
		'CL','CMCSA','CMA','CAG','CXO','COP','ED','STZ','COO','CPRT','GLW','CTVA','COST','COTY','CCI','CSX','CMI','CVS','DHI','DHR','DRI',
			'DVA','DE','DAL','XRAY','DVN','FANG','DLR','DFS','DISCA','DISCK','DISH','DG','DLTR','D','DOV','DOW','DTE','DUK','DRE','DD',
			'DXC','ETFC','EMN','ETN','EBAY','ECL','EIX','EW','EA','EMR','ETR','EOG','EFX','EQIX','EQR','ESS','EL','EVRG','ES','RE','EXC',
			'EXPE','EXPD','EXR','XOM','FFIV','FB','FAST','FRT','FDX','FIS','FITB','FE','FRC','FISV','FLT','FLIR','FLS','FMC','F','FTNT',
			'FTV','FBHS','FOXA','FOX','BEN','FCX','GPS','GRMN','IT','GD','GE','GIS','GM','GPC','GILD','GL','GPN','GS','GWW','HRB','HAL',
			'HBI','HOG','HIG','HAS','HCA','PEAK','HP','HSIC','HSY','HES','HPE','HLT','HFC','HOLX','HD','HON','HRL','HST','HWM','HPQ','HUM',
			'HBAN','HII','IEX','IDXX','INFO','ITW','ILMN','INCY','IR','INTC','ICE','IBM','IP','IPG','IFF','INTU','ISRG','IVZ','IPGP','IQV',
			'IRM','JKHY','J','JBHT','SJM','JNJ','JCI','JPM','JNPR','KSU','K','KEY','KEYS','KMB','KIM','KMI','KLAC','KSS','KHC','KR',
			'LB','LHX','LH','LRCX','LW','LVS','LEG','LDOS','LEN','LLY','LNC','LIN','LYV','LKQ','LMT','L','LOW','LYB','MTB','MRO','MPC',
			'MKTX','MAR','MMC','MLM','MAS','MA','MKC','MXIM','MCD','MCK','MDT','MRK','MET','MTD','MGM','MCHP','MU','MSFT','MAA','MHK','TAP',
			'MDLZ','MNST','MCO','MS','MOS','MSI','MSCI','MYL','NDAQ','NOV','NTAP','NFLX','NWL','NEM','NWSA','NWS','NEE','NLSN','NKE','NI',
			'NBL','JWN','NSC','NTRS','NOC','NLOK','NCLH','NRG','NUE','NVDA','NVR','ORLY','OXY','ODFL','OMC','OKE','ORCL','OTIS','PCAR','PKG',
			'PH','PAYX','PAYC','PYPL','PNR','PBCT','PEP','PKI','PRGO','PFE','PM','PSX','PNW','PXD','PNC','PPG','PPL','PFG','PG','PGR','PLD',
		'PRU','PEG','PSA','PHM','PVH','QRVO','PWR','QCOM','DGX','RL','RJF','RTX','O','REG','REGN','RF','RSG','RMD','RHI','ROK','ROL','ROP',
			'ROST','RCL','SPGI','CRM','SBAC','SLB','STX','SEE','SRE','NOW','SHW','SPG','SWKS','SLG','SNA','SO','LUV','SWK','SBUX','STT',
			'STE','SYK','SIVB','SYF','SNPS','SYY','TMUS','TROW','TTWO','TPR','TGT','TEL','FTI','TFX','TXN','TXT','TMO','TIF','TJX','TSCO',
			'TT','TDG','TRV','TFC','TWTR','TSN','UDR','ULTA','USB','UAA','UA','UNP','UAL','UNH','UPS','URI','UHS','UNM','VFC','VLO','VAR',
			'VTR','VRSN','VRSK','VZ','VRTX','VIAC','V','VNO','VMC','WRB','WAB','WMT','WBA','DIS','WM','WAT','WEC','WFC','WELL','WDC','WU',
			'WRK','WY','WHR','WMB','WLTW','WYNN','XEL','XRX','XLNX','XYL','YUM','ZBRA','ZBH','ZION','ZTS','^GSPC']

#Index of category of the stock from cat_list in same order as ticker list
categories =[1,2,2,2,3,4,3,3,5,6,7,2,8,3,1,8,9,2,2,1,2,3,6,7,4,4,10,5,8,6,1,6,7,7,9,6,7,2,1,2,3,3,3,2,7,1,11,9,3,3,5,10,3,7,7,4,6,3,3,5,9,
             8,11,8,7,7,2,2,5,2,7,1,5,5,9,2,2,3,3,1,11,3,10,7,5,2,5,5,1,1,7,9,3,8,2,6,4,2,8,7,4,11,5,7,10,2,7,1,3,7,7,3,10,7,6,10,3,10,4,7,
             10,11,11,6,10,2,1,3,8,10,10,9,1,1,2,5,2,5,2,1,1,2,11,11,9,7,4,4,4,5,5,6,1,8,6,6,9,8,3,7,8,1,5,8,6,2,4,1,6,11,1,9,9,9,10,6,6,7,6,
             5,1,6,11,3,4,1,9,1,3,7,6,7,3,3,3,1,8,5,3,1,1,4,4,7,8,5,5,3,1,1,10,5,5,2,7,3,7,1,5,11,5,5,7,5,2,9,11,2,10,11,3,5,11,2,5,1,10,9,
            1,3,2,7,1,1,2,1,1,2,2,1,3,7,3,8,4,8,3,2,7,3,2,9,3,1,1,10,2,1,7,3,1,10,7,3,10,9,11,3,5,10,10,5,1,2,3,10,5,5,3,5,2,7,8,4,5,1,7,5,
             8,7,11,11,7,5,7,8,1,3,10,3,5,2,2,2,7,2,5,3,3,3,9,5,10,10,10,7,7,8,3,7,2,7,11,3,4,5,8,4,4,6,1,5,6,11,5,1,7,1,3,5,6,8,3,5,5,11,1,
             4,11,3,1,1,8,1,3,3,3,1,7,10,2,2,2,10,11,6,11,7,8,6,7,10,7,9,7,6,9,5,5,3,1,3,2,5,7,1,9,9,2,7,1,2,1,1,1,1,5,5,7,3,9,11,3,8,6,3,
             8,9,3,9,1,6,1,1,5,7,2,2,7,7,3,10,4,7,4,5,5,3,11,2,3,1,2,5,5,5,1,1,7,7,4,10,9,5,7,5,5,1,1,2,1,1,2,7,5,11,2,9,3,1,4,2,4,3,9,8,7,1,
             10,10,4,1,2,6,7,9,3,3,8,9,5,11,7,5,6,3,3,1,5,3,2,7,2,12]

cat_list = ['Industrials','Health_Care','Information_Technology','Communication_Services','Consumer_Discretionary','Utilities', 'Financials','Materials','Real_Estate','Consumer_Staples','Energy','SNP_500_Index']

#Creating a dictionary with categories as keys and list of stocks in that category as values
cat_dict = {}
for i in cat_list:
  cat_dict[i] = []
for i in range(len(categories)):
  cat_dict[cat_list[categories[i]-1]].append(ticker[i])

# COMMAND ----------

# DBTITLE 1,Retrieving the closing price data of individual stocks from raw data
import pandas as pd
df = pd.read_csv("/databricks/driver/stock_and_covid_data/raw_data.csv",header = [0,1],index_col=0)
for i in ticker:
	df[i] = df['Close'][i] #The columns are multi-indexed
df = df[ticker]
df=df.reset_index()
df=df.iloc[1:-1]
df=df.droplevel(1,axis=1) #Converting to single-indexed columns
df.to_csv("/databricks/driver/stock_and_covid_data/price_data.csv",index=False)

# COMMAND ----------

# DBTITLE 1,Creating Databricks filesystem folder and copying data
dbutils.fs.rm("stock_and_covid_data",recurse=True)
dbutils.fs.mkdirs("stock_and_covid_data")
dbutils.fs.cp("file:/databricks/driver/stock_and_covid_data/price_data.csv", "dbfs:/stock_and_covid_data/")
dbutils.fs.cp("file:/databricks/driver/stock_and_covid_data/stock_info.csv", "dbfs:/stock_and_covid_data/")
dbutils.fs.cp("file:/databricks/driver/stock_and_covid_data/us_confirmed.csv", "dbfs:/stock_and_covid_data/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls stock_and_covid_data

# COMMAND ----------

# MAGIC %md
# MAGIC Creating DataFrames for each of the datasets (closing price date, stock info, covid-19 confirmed cases in the US):
# MAGIC 
# MAGIC I inferred the schema from the datasets since they had large number of columns (504 different stocks). I later extracted the columns that I needed and casted them to required datatypes

# COMMAND ----------

# DBTITLE 1,Creating DataFrames for each of the datasets 
info_df = spark.read.format("csv").option("inferSchema","true").option("header", "true").option("delimiter", ",").load("dbfs:/stock_and_covid_data/stock_info.csv")
price_df = spark.read.format("csv").option("inferSchema","true").option("header", "true").option("delimiter", ",").load("dbfs:/stock_and_covid_data/price_data.csv")
covid_df = spark.read.format("csv").option("inferSchema","true").option("header", "true").option("delimiter", ",").load("dbfs:/stock_and_covid_data/us_confirmed.csv")

# COMMAND ----------

# DBTITLE 1,Checking the schema of the dataframes
info_df.schema

# COMMAND ----------

price_df.schema

# COMMAND ----------

covid_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC Extracting the rows that contain only the market cap of the stocks from info_df and typecasting them from string to float

# COMMAND ----------

# DBTITLE 1,Market-Cap extraction
from pyspark.sql.functions import col
info_df = info_df.where("Param=='marketCap'")
info_df = info_df.drop('Param')
info_df= info_df.select([col(c).cast("float") for c in info_df.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC Aggregating US Covid-19 confirmed cases by date and Calculating new cases

# COMMAND ----------

# DBTITLE 1,Processing Covid-19 data
from pyspark.sql import functions as F
from pyspark.sql.window import Window
covid_df = covid_df.selectExpr("to_date(Date) as Date","Case as Cases")
covid_df.createOrReplaceTempView("us_cases")
covid_df = spark.sql("SELECT Date,SUM(Cases) AS Cases FROM us_cases GROUP BY Date ORDER BY Date")
my_window = Window.partitionBy().orderBy("Date")
covid_df = covid_df.withColumn("prev_value", F.lag(covid_df.Cases).over(my_window)) #Creating a column for number of cases for previous day
covid_df = covid_df.withColumn("New_Cases", F.when(F.isnull(covid_df.Cases - covid_df.prev_value), covid_df.Cases).otherwise(covid_df.Cases - covid_df.prev_value)) #Defining New_Cases column as cases-prev_value. Since prev_values is null in first column, that difference is same as Cases.
covid_df = covid_df.drop("prev_value")
#covid_df = covid_df.selectExpr("Date","Cases","New_Cases")
covid_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Converting timestamp in price data to date
# MAGIC 
# MAGIC Also, from the graphs below during analysis, I realised that 5th of December, 2018 was present in the price data though it was a holiday and the stock prices are nulls for it. So, excluding it.
# MAGIC 
# MAGIC The prices for certain stocks weren't populated in the data for certain dates, the reasons for which I am unaware of. Hence filling those columns with 0.

# COMMAND ----------

# DBTITLE 1,Cleaning price data
from pyspark.sql.functions import to_date
from pyspark.sql.functions import lit
price_df=price_df.withColumnRenamed("Date","old_date")
price_df=price_df.selectExpr("to_date(old_date) as Date","*").drop("old_date")
price_df = price_df.withColumn("remove_date",to_date(lit("2018-12-05")))
price_df = price_df.selectExpr("*").where("Date != remove_date").drop("remove_date")
price_df=price_df.na.fill(0)

# COMMAND ----------

# DBTITLE 1,Saving processed files to disk 
dbutils.fs.rm("processed_stock_and_covid_data",recurse=True)
dbutils.fs.mkdirs("processed_stock_and_covid_data")
info_df.write.parquet("dbfs:/processed_stock_and_covid_data/info_df.parquet")
price_df.write.parquet("dbfs:/processed_stock_and_covid_data/price_df.parquet")
covid_df.write.parquet("dbfs:/processed_stock_and_covid_data/covid_df.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint-1
# MAGIC Checkpoint - The position from which you can run the code again if something goes wrong below this point and you had to change code.

# COMMAND ----------

# DBTITLE 1,Reading Dataframes from disk
info_df = spark.read.parquet("dbfs:/processed_stock_and_covid_data/info_df.parquet")
price_df = spark.read.parquet("dbfs:/processed_stock_and_covid_data/price_df.parquet")
covid_df = spark.read.parquet("dbfs:/processed_stock_and_covid_data/covid_df.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Calculating the weights for stock categories to calculate the cap-weighed index for each category.
# MAGIC 
# MAGIC Weight of a stock = (Market-Cap of that stock) / (Total Market-Cap of all the stocks in that category)

# COMMAND ----------

# DBTITLE 1,Weights
mc = info_df.collect()[0] #collecting dataframe for faster access in loop
cat_weights = {} #dictionary for stock weights category wise.
for i in cat_list:
  cat_weights[i]=[]
  if i == 'SNP_500_Index':
    cat_weights[i].append(1) #adding 1 since only one stock in this category
    continue
  market_cap = 0
  for j in cat_dict[i]:
    market_cap = market_cap+mc[j] #Calculating total market cap for each category
  for j in cat_dict[i]:
    cat_weights[i].append(mc[j]/market_cap) #weight of each stock

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Index Dataframe by calculating the Markt Cap-Weighed Indexes for each Sector in S&P 500
# MAGIC 
# MAGIC Index = Sum(Weight x Stock Price), of all the stocks in the category

# COMMAND ----------

# DBTITLE 1,Indexes
import pandas as pd
import numpy as np
price_pdf = price_df.toPandas() #Collecting price dataframe to memory as pandas dataframe
index_pdf = pd.DataFrame() #Creating Index Dataframe
index_pdf['Date'] = price_pdf['Date']
for i in cat_list:
  price_data = price_pdf[cat_dict[i]].to_numpy()
  index_price = np.matmul(price_data,np.array(cat_weights[i])) #Calculating category Index
  if i == 'SNP_500_Index':
    label = i
  else:
    label = i+'_Index'
  index_pdf[label] = index_price
index_df = spark.createDataFrame(index_pdf) #Converting pandas Dataframe to spark Dataframe

# COMMAND ----------

# DBTITLE 1,Saving the processed index_df to disk
index_df.write.parquet("dbfs:/processed_stock_and_covid_data/index_df.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Checkpoint-2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Predictions and Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC Installing *fbprophet* library for Time Series Analysis, since SparkML doesn't have any library for Time Series Analysis. R has some efficient and accurate Time Series libraries (like *forecast*), but I wanted to stick to Python to maintain uniformity and fbprophet uses scikit-learn like syntax which most users are comfortable with. 

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install fbprophet

# COMMAND ----------

import logging
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

# DBTITLE 1,Reading Dataframes from disk
info_df = spark.read.parquet("dbfs:/processed_stock_and_covid_data/info_df.parquet")
price_df = spark.read.parquet("dbfs:/processed_stock_and_covid_data/price_df.parquet")
covid_df = spark.read.parquet("dbfs:/processed_stock_and_covid_data/covid_df.parquet")
index_df = spark.read.parquet("dbfs:/processed_stock_and_covid_data/index_df.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Checking the starting date of covid-19 dataset to divide Index_df dataframe into train and test dataframes.

# COMMAND ----------

covid_df.selectExpr("min(Date)").show()

# COMMAND ----------

# DBTITLE 1,Train-Test Split
train_dfs = {} #dictionary for training dataframes category wise.
test_dfs = {} #dictionary for test(actual data) dataframes category wise.
index_df = index_df.withColumn("start_date",to_date(lit("2020-1-22")))
index_df.cache()
for i in cat_list:
  if i == 'SNP_500_Index':
    target = i+' as y'
    test_label=i
  else:
    target = i+'_Index as y'
    test_label=i+'_Index'
  train_dfs[i] = index_df.selectExpr("Date as ds",target).where("Date<start_date") #creating train and test dataframes for each category index based on start date of covid-19 cases
  test_dfs[i] = index_df.selectExpr("Date",test_label).where("Date>=start_date")

# COMMAND ----------

# DBTITLE 1,Model fitting
from fbprophet import Prophet
model ={} #dictionary for models category wise.
for i in cat_list:
  model[i] = Prophet(interval_width=0.95,
      growth='linear',
      daily_seasonality=False,
      weekly_seasonality=False,
      yearly_seasonality=True,
   )
  dat = train_dfs[i].toPandas()
  model[i].fit(dat)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking last date in index dataset for prediction

# COMMAND ----------

test_dfs['SNP_500_Index'].selectExpr("max(Date)").show()

# COMMAND ----------

# MAGIC %md
# MAGIC There are 100 days from 22nd January to 30th April. So making predictions for 100 days

# COMMAND ----------

# DBTITLE 1,Forecasting
forecast_dfs={} #dictionary for forecast dataframes category wise.
for i in cat_list:
  future_pd = model[i].make_future_dataframe(periods=100,freq='d',include_history=True)
  forecast_dfs[i]=model[i].predict(future_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC Plotting time series predictions for the training set as well as future dates.
# MAGIC 
# MAGIC The blue plots are the predictions and the red ones are the actual data for the whole time frame. 
# MAGIC The black dots represent the actual data and the blue line represents the predictions. The light blue region below and above the blue line represents the 95% Confidence Interval of prediction.
# MAGIC 
# MAGIC The predictions have an upward trend in all the indexes. But the actual indexes drops heavily during the covid-19 phase. 

# COMMAND ----------

# DBTITLE 1,Plotting the Series, Forecast and Actual Indexes
import matplotlib.pyplot as plt
%matplotlib inline
plt.rcParams['figure.figsize']=(12,10)
plt.style.use('ggplot')
pd.plotting.register_matplotlib_converters()

index_list=[]
for i in cat_list:
  if i == 'SNP_500_Index':
    label = i
    index_list.append(label)
  else:
    label = i+'_Index'
    index_list.append(label)
  predict_fig = model[i].plot( forecast_dfs[i], xlabel='date', ylabel=label+'_Predicted')
  display(predict_fig)
high_index_list = ['Consumer_Discretionary_Index','Communication_Services_Index','SNP_500_Index']
low_index_list = list(set(index_list).difference(set(high_index_list)))
df = index_df.toPandas()
df.plot(x="Date",y=high_index_list.pop(),title='S&P 500 Index')
df.plot(x="Date",y=low_index_list,title='Indexes')
df.plot(x="Date",y=high_index_list,title='Indexes')

# COMMAND ----------

# MAGIC %md
# MAGIC Since all the dates are not present in the price data (weekends and holidays exempt) I joined(Inner) the datasets on Date to get the data only for common dates

# COMMAND ----------

# DBTITLE 1,Joins
index_covid_dfs={} #dictionary for joined dataframes category wise.
covid_df.cache()
for i in cat_list:
  label = "yhat as Pred_"+i+"_Index"
  test_label = i+"_Index"
  if i == 'SNP_500_Index':
    label = "yhat as Pred_"+i
    test_label = i
  forecast_df =spark.createDataFrame(forecast_dfs[i]) #creating spark dataframe from pandas dataframe
  forecast_df = forecast_df.selectExpr("to_date(ds) as Date",label)
  test_df = test_dfs[i]
  temp_df = forecast_df.join(test_df,on=['Date'],how='inner') #joining forecast and test dataframes on Date
  index_covid_dfs[i]=temp_df.join(covid_df,on=['Date'],how='inner').sort("Date") #joining temp and covid dataframes on Date

# COMMAND ----------

# MAGIC %md
# MAGIC Since S&P 500 Index is the cumulative Index of all the indexes we defined, let us compare it with Total covid-19 cases for each day. 

# COMMAND ----------

# DBTITLE 1,Total Cases vs S&P 500 Index
plt.rcParams['figure.figsize']=(12,10)
plt.style.use('ggplot')
pd.plotting.register_matplotlib_converters()
index_covid_df=index_covid_dfs['SNP_500_Index'].toPandas()
index_covid_df.plot(x='Date',y='Cases',title = "Cases")
index_covid_df.plot(x='Date',y='SNP_500_Index',title = 'SNP_500_Index')
index_covid_df.plot(y='SNP_500_Index',x='Cases',title = "S&P 500 vs Cases")

# COMMAND ----------

# MAGIC %md
# MAGIC Since the total number of cases is continuously increasing, it appears as if the there is no particular correlation for the two plots. Part of the reason might also be the range of total cases which spans several orders - from 0 to about a Million. Also, since the behaviour of the Index is not predefined, it makes more sense to plot the deviation from expected behaviour with respect to new cases for a day. Hence I am plotting the percentage difference between the Prediction and Actual stock indexes and the new cases for each date in test dataframes.

# COMMAND ----------

# DBTITLE 1,Plotting % difference from prediction to actual
plt.rcParams['figure.figsize']=(12,10)
plt.style.use('ggplot')
pd.plotting.register_matplotlib_converters()
new_cases = covid_df.toPandas()
new_cases.plot(x='Date',y='New_Cases',title = "New Cases") #plot new cases vs dates
for i in cat_list:
  label1 = i+"_Index"
  label2 = "Pred_"+i+"_Index"
  titl = "% Difference of "+i+" Index"
  if i == 'SNP_500_Index':
    label1 = i
    label2 = "Pred_"+i
    titl = "% Difference of "+i
  diff=index_covid_dfs[i].withColumn("%Diff",(col(label2)-col(label1))*100/col(label2)).toPandas() #Calculate percentage difference in Index from prediction
  diff.plot(x='Date',y='%Diff',title=titl) #Plot percentage difference in index vs date

# COMMAND ----------

# MAGIC %md
# MAGIC **Summary:**  
# MAGIC The first plot above is the trend for new cases. The plots below are the trends for percentage difference in each index. We can see that the percentage difference increased, meaning the indexes crashed, as long as the new cases increased. Once the new cases curve started flattening(a bit before that, which is when the Shelter in place was enforced in many states in the US resulting in the curve flattening), we see the percentage difference started to fall indicating that the indexes were increasing and the economy was reviving. 
# MAGIC 
# MAGIC So, we can see that the Consumer_Discretionary_Index, Health_Care_Index, Real_Estate_Index and Consumer_Staples_Index recovered most. But looking at the potential prediction and the recovery rate, Industrials, Information_Technology, Communication_Services and Financials are the fields that have the highest immediate potential for growth.Hence, I think investing in those fields is probably profitable given the above facts.
# MAGIC 
# MAGIC Though the Energy Index looks like it has not revived yet, if we look at the series plot from above(the plot in grey), it has a downward trend. Hence, I wouldn't suggest investing in the field of Energy.
