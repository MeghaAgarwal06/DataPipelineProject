import sys

from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_config, load_survey_df, clean_data
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