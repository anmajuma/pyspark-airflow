from pyspark.sql import SparkSession


def createSparkSession():
    spark = SparkSession.builder \
        .appName('lakeHouseApp') \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    return spark