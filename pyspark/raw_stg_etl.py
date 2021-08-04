import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,to_timestamp
from pyspark.sql.types import TimestampType,DateType
import pyspark.sql.functions as F
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--func", help="provide list of funcs")
args = parser.parse_args()
if args.func:
    infunc = args.func
    

#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULT']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("Spark ETL") \
        .getOrCreate()
    return spark

#s3://dend-data-raw/yelp-data/yelp_academic_dataset_checkin.json
def process_yelp_checkin(spark,input_data="s3://dend-data-raw/",output_data ="s3://dend-data-landing/"):
    """
    arg:  spark, inputdata,outputdata
    """
    print(spark,input_data,output_data,input_data+"yelp-data/yelp_academic_dataset_checkin.json")
    yelp_checkin = spark.read.json(input_data+"yelp-data/yelp_academic_dataset_checkin.json")
    yelp_checkin.createOrReplaceTempView("checkin")
    out_checkin = spark.sql("select business_id,explode(split(date,',')) as date from checkin")
    out_checkin.createOrReplaceTempView("checkin_exploded")
    out_checkin2 = spark.sql("select business_id,CAST(UNIX_TIMESTAMP(date, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) as date from checkin_exploded")
    out_checkin2.write.option("header", "true").mode("overwrite").parquet(output_data+"stg_checkin")

def process_yelp_users(spark,input_data="s3://dend-data-raw/",output_data ="s3://dend-data-landing/"):
    """
    arg:  spark, inputdata,outputdata
    """
    #print(spark,input_data,output_data)
    yelp_checkin = spark.read.json(input_data+"yelp-data/yelp_academic_dataset_user.json")
    yelp_checkin.createOrReplaceTempView("users")
    yelp_users_ex_friends=spark.sql("select * from users").drop('friends')
    yelp_users_ex_friends = yelp_users_ex_friends.withColumn("yelping_since",to_timestamp(col("yelping_since"))) #convert varchar to timestamp
    #yelp_users_ex_friends=yelp_users_ex_friends.na.fill({'_corrupt_record': 'no_err'})
    yelp_users_friends = spark.sql("select user_id,explode(split(friends,',')) as friend_id from users")
    yelp_users_ex_friends.write.option("header", "true").mode("overwrite").parquet(output_data+"stg_users") 
    yelp_users_friends.write.option("header", "true").mode("overwrite").parquet(output_data+"stg_users_friends") 

def main(func):
    """
    arg: take list of functions, and corresponding params and execute sequentially
    """
    spark = create_spark_session()
    if func =='process_yelp_checkin':
        process_yelp_checkin(spark)
    elif func =='process_yelp_users':
        process_yelp_users(spark)
    else:
        return "Error. Function Not Exists"

if __name__ == "__main__":
    main(infunc)
    