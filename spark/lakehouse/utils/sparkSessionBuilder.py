from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def createSparkSession():
    # spark = SparkSession.builder \
    #     .appName(app_name) \
    #     .enableHiveSupport() \
    #     .getOrCreate()
    # spark.sparkContext.setLogLevel('ERROR')
    # return spark

    builder = (
    SparkSession.builder
        .appName('lakeHouseApp')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark