import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
#        "crime_id": "183653746",
#        "original_crime_type_name": "Passing Call",
#        "report_date": "2018-12-31T00:00:00.000",
#        "call_date": "2018-12-31T00:00:00.000",
#        "offense_date": "2018-12-31T00:00:00.000",
#        "call_time": "23:49",
#        "call_date_time": "2018-12-31T23:49:00.000",
#        "disposition": "HAN",
#        "address": "3300 Block Of 20th Av",
#        "city": "San Francisco",
#        "state": "CA",
#        "agency_id": "1",
#        "address_type": "Common Location",
#        "common_location": "Stonestown Galleria, Sf"

#https://spark.apache.org/docs/2.1.2/api/python/_modules/pyspark/sql/types.html
schema = StructType([
    StructField("crime_id", IntegerType()),
    StructField("original_crime_type_name", StringType()),
    StructField("report_date", TimestampType()),
    StructField("call_date", TimestampType()),
    StructField("offense_date", TimestampType()),
    StructField("call_time", StringType()),
    StructField("call_date_time", TimestampType()),
    StructField("disposition", StringType()),
    StructField("address", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("agency_id", StringType()),
    StructField("address_type", StringType()),
    StructField("common_location", StringType())
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","localhost:9092") \
        .option("subscribe","sf-streamed-data") \
        .option("startingOffsets","earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown","true") \
        .load()

    # Show schema for the incoming resources for checks
    print('Printing the schema for the topic sf-streamed-data')
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    print("The schema for the service_table")
    service_table.printSchema()

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table \
        .select(["original_crime_type_name","disposition","call_date_time"]) \
        .distinct() 
        
    
    
    print("The schema for the distinct_table")
    distinct_table.printSchema()

    # count the number of original crime type
    agg_df = distinct_table \
        .withWatermark("call_date_time", "30 minutes") \
        .groupby(psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"), distinct_table.original_crime_type_name) \
        .count()
    

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    print('Printing the aggregation for the ingested batch')
    query = agg_df \
        .writeStream \
        .format('console') \
        .outputMode('Complete') \
        .option("truncate","false") \
        .start()


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df,agg_df.disposition == radio_code_df.disposition,"left")
    
    query2 = join_query \
        .writeStream \
        .format('console') \
        .outputMode('Complete') \
        .option("truncate","false") \
        .start()


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()

    logger.info("Spark started")
    spark.sparkContext.setLogLevel("WARN")

    run_spark_job(spark)

    spark.stop()
