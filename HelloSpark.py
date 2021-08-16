import sys

from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_config, load_survey_df, count_by_country
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

    schemaDDL = """domain_userid STRING, network_userid STRING, domain_sessionid STRING, useragent STRING, os_name STRING,os_family STRING,br_name STRING,br_family STRING,br_version STRING,
        br_type STRING, derived_tstamp STRING,collector_tstamp STRING,page_urlscheme STRING, page_urlhost STRING,page_urlport INT, page_urlpath STRING, refr_urlscheme STRING, refr_urlhost STRING, refr_urlport INT, refr_urlpath STRING,
        app_id STRING, user_ipaddress STRING,page_view_id STRING, member_id STRING, member_type STRING, article_id STRING, article_type STRING,article_primary_category STRING"""

    complete_df = spark.read. \
        option("header", "true"). \
        format("csv").schema(schemaDDL). \
        load(sys.argv)

    complete_df.createOrReplaceTempView("compsurvey_tbl")
    complete_df.show()
    logger.info("Finished HelloSpark")

    # Data cleansing
    # 1.Remove Unwanted fields
    # 2.Remove duplicate records
    # 3.Identify junk records and delete
    # 4.Converting string to datetime

    complete_df.drop(complete_df.app_id)

    # removing duplicates from data
    distinctDF = complete_df.distinct()
    print("Distinct count: ", distinctDF.count())

    # converting string to datetime formats
    df = spark.sql(
        " select  substr(derived_tstamp,1,22) as derived_tstp ,substr(collector_tstamp,1,22) as collector_tstp,* FROM compsurvey_tbl where page_view_id is not null")

    dfNew = df.select(("domain_userid"), col("network_userid"), col("domain_sessionid"), col("useragent"),
                      col("os_name"), col("os_family"), col("br_name"), col("br_family"), col("br_version"),
                      col("br_type"), col("page_urlscheme"), col("page_urlhost"), col("page_urlport"),
                      col("page_urlpath"), col("refr_urlscheme"), col("refr_urlhost"), col("refr_urlport"),
                      col("refr_urlpath"), col("user_ipaddress"), col("page_view_id"), col("member_id"),
                      col("member_type"), col("article_id"), col("article_type"), col("article_primary_category"),
                      to_timestamp(col("derived_tstp"), "yyyy-MM-dd HH:mm:ss.SS").alias("derived_tstamp"),
                      to_timestamp(col("collector_tstp"), "yyyy-MM-dd HH:mm:ss.SS").alias("collector_tstamp"))
    # dfNew.show(1,False)
    dfNew.createOrReplaceTempView("final_tbl")

    # Event  modelling for workflows

    df2020 = spark.sql(
        "select *f rom final_tbl where (derived_tstamp>'2019-12-31 23:59:59' and derived_tstamp<='2020-12-31 23:59:59')")
    df2020.createOrReplaceTempView("year2020tbl")

    # Gather Basic information for 2020 year
    basicInfoDf = spark.sql(
        "SELECT  COUNT(DISTINCT domain_userid) AS user_count,COUNT(DISTINCT domain_sessionid) AS session_count,count(page_view_id),MIN(derived_tstamp) AS first_event,MAX(derived_tstamp) AS last_event from year2020tbl ")

    pgViewPerSessionDF = spark.sql(
        "SELECT domain_userid,domain_sessionid,min(derived_tstamp) as st_session_tstp,max(derived_tstamp) as end_session_tstp,max(derived_tstamp)-min(derived_tstamp) as session_duration,count(page_view_id) as pageviews from year2020tbl  group by domain_userid,domain_sessionid  order by count(page_view_id) asc ")

    # Obtain average session duration of sessions
    pgViewPerSessionDF.createOrReplaceTempView("l_tbl")
    spark.sql("select sum(session_duration)/count(*) from l_tbl")