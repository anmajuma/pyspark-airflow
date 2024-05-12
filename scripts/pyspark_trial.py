from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
def main():

    source_bucket = "wba"

    spark = SparkSession.builder \
        .appName("SparkAirFlowDemo") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
    ])
 
    df = spark.createDataFrame(data=data2,schema=schema)
    df.printSchema()
    df.show(truncate=False)

    # input_path = f"s3a://{source_bucket}/test-data/people-100.csv"
    delta_path = f"s3a://{source_bucket}/delta/wba/tables/"
    df.write.format("delta").option("delta.columnMapping.mode", "name").save(delta_path)

    # # spark.sql("DROP SCHEMA IF EXISTS wba CASCADE")

    # # spark.sql("CREATE DATABASE IF NOT EXISTS wba")
    # # spark.sql("USE wba")

    # df = spark.read.csv(input_path, header=True, inferSchema=True)

    # df.show()
    # df.write.format("delta").option("delta.columnMapping.mode", "name").save(delta_path)
    # df.write.format("delta").option("delta.columnMapping.mode", "name")\
    #     .option("path", f'{delta_path}/test_table')\
    #     .saveAsTable("wba.test_table")

    # dt = DeltaTable.forName(spark, "wba.test_table")

    # dt.toDF().show()


if __name__ == "__main__":
    main()