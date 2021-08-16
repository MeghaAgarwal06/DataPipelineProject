import configparser
from pyspark import SparkConf
from pyspark.sql import *


def get_spark_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)

    return spark_conf


def load_survey_df(spark, data_file):
    schemaDDL = """domain_userid STRING, network_userid STRING, domain_sessionid STRING, useragent STRING, os_name STRING,os_family STRING,br_name STRING,br_family STRING,br_version STRING,
            br_type STRING, derived_tstamp STRING,collector_tstamp STRING,page_urlscheme STRING, page_urlhost STRING,page_urlport INT, page_urlpath STRING, refr_urlscheme STRING, refr_urlhost STRING, refr_urlport INT, refr_urlpath STRING,
            app_id STRING, user_ipaddress STRING,page_view_id STRING, member_id STRING, member_type STRING, article_id STRING, article_type STRING,article_primary_category STRING"""

    return spark.read. \
        option("header", "true"). \
        format("csv").schema(schemaDDL). \
        load(data_file)



def clean_data(spark,complete_df):
    complete_df.createOrReplaceTempView("compsurvey_tbl")
    complete_df.drop(complete_df.app_id)

    # removing duplicates from data
    distinctDF = complete_df.distinct()
    # print("Distinct count: ", distinctDF.count())

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
    return dfNew
