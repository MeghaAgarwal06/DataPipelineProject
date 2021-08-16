import sys

from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_config, load_survey_df, clean_data
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import functions

if __name__ == "__main__":

    print("Starting hello spark")
    conf = get_spark_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("usage:HelloSpark data_raw_data000000000000")
        sys.exit(-1)


    #Loading the data into a data frame

    complete_df=load_survey_df(spark,sys.argv[1])


    #complete_df.show()
    logger.info("Finished Loading the data to a dataframe")

    # Data cleansing
    # 1.Remove Unwanted fields
    # 2.Remove duplicate records
    # 3.Identify junk records and delete
    # 4.Converting string to datetime

    dfNew=clean_data(spark,complete_df)
    dfNew.createOrReplaceTempView("final_tbl")

    # Event  modelling for workflows
    # Picking historical year as 2020
    df2020 = spark.sql(
        "select *f rom final_tbl where (derived_tstamp>'2019-12-31 23:59:59' and derived_tstamp<='2020-12-31 23:59:59')")
    df2020.createOrReplaceTempView("year2020tbl")

    # Gather Basic information for 2020 year like number of users, sessions, page views
    basicInfoDf = spark.sql(
        "SELECT  COUNT(DISTINCT domain_userid) AS user_count,COUNT(DISTINCT domain_sessionid) AS session_count,count(page_view_id),MIN(derived_tstamp) AS first_event,MAX(derived_tstamp) AS last_event from year2020tbl ")

    pgViewPerSessionDF = spark.sql(
        "SELECT domain_userid,domain_sessionid,min(derived_tstamp) as st_session_tstp,max(derived_tstamp) as end_session_tstp,max(derived_tstamp)-min(derived_tstamp) as session_duration,count(page_view_id) as pageviews from year2020tbl  group by domain_userid,domain_sessionid  order by count(page_view_id) asc ")

    # Obtain average session duration of sessions
    pgViewPerSessionDF.createOrReplaceTempView("l_tbl")
    spark.sql("select sum(session_duration)/count(*) from l_tbl")


    # Obtain top Referrers
    allReferrer = spark.sql(
        "select FIRST_VALUE(refr_urlhost)OVER (  PARTITION BY domain_sessionid  ORDER BY domain_sessionid, collector_tstamp) AS first_refr_urlhost  from year2020tbl where (refr_urlhost is not null or refr_urlhost!='/') ")
    allReferrer.createOrReplaceTempView("referr_tbl")
    referDf = spark.sql("select  distinct  first_refr_urlhost from referr_tbl ")
    referDf = spark.sql(
        "select  CASE WHEN first_refr_urlhost LIKE '%google%' THEN 'GOOGLE' WHEN first_refr_urlhost LIKE '%facebook%' THEN 'FACEBOOK'  WHEN first_refr_urlhost LIKE '%xyz%' THEN 'XYZ'  WHEN first_refr_urlhost LIKE '%fairfax%' THEN 'FAIRFAX'   WHEN first_refr_urlhost LIKE '%duck%' THEN 'DUCKDUCKGO'   WHEN first_refr_urlhost LIKE '%msn%' THEN 'msn'  WHEN first_refr_urlhost LIKE '%yahoo%' THEN 'yahoo'   WHEN first_refr_urlhost LIKE '%bing%' THEN 'bing'  else 'Others' END AS referrer from referr_tbl ")
    refer = referDf.groupBy("referrer").count()
    referPlotDF = refer.orderBy(col("count").desc())


    #Obtain the device types used to access the website
    devicesDF = spark.sql(
        "select CASE WHEN upper(os_name) LIKE '%IPAD%'THEN 'TABLET' WHEN upper(os_name) LIKE '%TABLET%'THEN 'TABLET' WHEN upper(os_name) LIKE '%WINDOWS%'THEN 'PC'WHEN upper(os_name) LIKE '%MAC%'THEN 'PC' WHEN upper(os_name) LIKE '%IPHONE%'THEN 'MOBILE'WHEN upper(os_name) LIKE '%MOBILE%'THEN 'MOBILE'WHEN upper(os_name) LIKE '%ANDROID%'THEN 'MOBILE' ELSE 'Unknown'END AS devices from year2020tbl order by os_name desc")
    deviceGroups = devicesDF.groupBy("devices").count()
    deviceGroupPlotDf = deviceGroups.orderBy(col("count").desc())


    # Visualization of data on dashboard
    DeviceGrpPandas = deviceGroupPlotDf.toPandas()
    DeviceGrpPandas.plot(x='devices', y='count', kind='bar')
    plt.title("Users by Devices")

    topRefrPandas = referPlotDF.toPandas()
    topFiverefPandas = topRefrPandas.head(5)

    topFiverefPandas.plot(x='referrer', y='count', kind='barh')
    plt.title("Top Referrer")



