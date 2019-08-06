import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession, SQLContext

import utils


BOOTSTRAP_SERVERS = '10.156.0.3:6667,10.156.0.4:6667,10.156.0.5:6667'
GROUP_ID = 'dborisov_spark_task_2'
RESULT_TABLE = 'dborisov.announcement_parsed'
SPARK_APP_NAME = 'dborisov_task_2_mles_announcements'
SPARK_JARS_PACKAGES = 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0'
SPARK_YARN_QUEUE = 'dborisov'
TOPIC_NAME = 'mles.announcements'


spark = SparkSession \
    .builder \
    .appName(SPARK_APP_NAME) \
    .master('yarn') \
    .config('spark.yarn.queue', SPARK_YARN_QUEUE) \
    .config('spark.executor.cores', '3') \
    .config('spark.executor.memory', '6g') \
    .config('spark.jars.packages', SPARK_JARS_PACKAGES) \
    .enableHiveSupport() \
    .getOrCreate()

kafka_schema = T.StructType([
    T.StructField('address', T.StringType()),
    T.StructField('announcementid', T.StringType()),
    T.StructField('category', T.StringType()),
    T.StructField('dateInserted', T.TimestampType()),
    T.StructField('description', T.StringType()),
    T.StructField('floorNumber', T.StringType()),
    T.StructField('floorsCount', T.StringType()),
    T.StructField('lat', T.DoubleType()),
    T.StructField('lng', T.DoubleType()),
    T.StructField('price', T.DoubleType()),
    T.StructField('priceType', T.StringType()),
    T.StructField('roomsCount', T.StringType()),
    T.StructField('status', T.IntegerType()),
    T.StructField('totalArea', T.DoubleType()), 
])


def main():
    df = spark \
        .read \
        .format('kafka') \
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS) \
        .option('subscribe', TOPIC_NAME) \
        .option('group.id', GROUP_ID) \
        .option('startingOffsets', utils.get_starting_offsets(TOPIC_NAME)) \
        .load() \
        .cache()

    ads_data = df.select(
        F.from_json(F.col('value').cast('string'), kafka_schema) \
            .alias('json')
    ) \
        .select('json.*') \
        .withColumn('announcementid', F.col('announcementid').cast('long')) \
        .withColumn('floorNumber', F.col('floorNumber').cast('int')) \
        .withColumn('floorsCount', F.col('floorsCount').cast('int')) \
        .withColumn('roomsCount', F.col('roomsCount').cast('int')) \
        .withColumn('ptn_dadd', F.col('dateInserted').cast(T.DateType()))

    ads_data \
        .write \
        .format('orc') \
        .mode('append') \
        .partitionBy('ptn_dadd') \
        .saveAsTable(RESULT_TABLE)

    partition_offsets_mapping = {
        str(partition): offset + 1
        for partition, offset in df.groupBy('partition').agg({'offset': 'max'}).collect()
    }

    utils.dump_offsets(TOPIC_NAME, partition_offsets_mapping)
    
    
if __name__ == '__main__':
    main()
