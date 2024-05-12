import pyspark.sql.functions as F
from delta.tables import *
from pyspark.sql.types import *

cloud_storage_bucket = "wba"
auditlogpath = "s3a://%s/auditlog" % cloud_storage_bucket

spark = SparkSession.builder \
        .appName("SparkAirFlowDemo") \
        .enableHiveSupport() \
        .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Create an expected schema
jobcolumns = StructType([StructField('JobName',
                                  StringType(), True),
                    StructField('SourceSystemName',
                                StringType(), True),
                     StructField('SourceTableName',
                                StringType(), True),
                    StructField('JobID',
                                StringType(), True),
                    StructField('RowsIngested',
                                IntegerType(), True),   
                    StructField('RowsRejected',
                                IntegerType(), True),      
                    StructField('RowsUpserted',
                                IntegerType(), True),                                 
                    StructField('JobStartTime',
                                TimestampType(), True),
                    StructField('JobEndTime',
                                TimestampType(), True),
                    StructField('JobStatus',
                                StringType(), True),      
                    StructField('ErrorMessage',
                                StringType(), True)                                                                                                                                                                                                                                                                                                                                                               
                                ])



# Create an expected schema
taskcolumns = StructType([StructField('JobID',
                                  StringType(), True),
                    StructField('TaskName',
                                StringType(), True),
                    StructField('TaskID',
                                StringType(), True),
                    StructField('TaskStartTime',
                                TimestampType(), True),
                    StructField('TaskEndTime',
                                TimestampType(), True),                                                                                                
                    StructField('RowsIngested',
                                IntegerType(), True),   
                    StructField('RowsRejected',
                                IntegerType(), True),      
                    StructField('RowsUpserted',
                                IntegerType(), True),      
                    StructField('TaskStatus',
                                StringType(), True),      
                    StructField('ErrorMessage',
                                StringType(), True)                                                                                                                                                                                                                                                                                                                                                               
                                ])
 


  # Create an empty RDD with expected schema
emp_RDD = spark.sparkContext.emptyRDD()
jobdf = spark.createDataFrame(data = emp_RDD,schema = jobcolumns)
taskdf = spark.createDataFrame(data = emp_RDD,schema = taskcolumns)
try:
    existingjobdf = spark.read.load(auditlogpath+'/jobauditlog')
    print("Job Audit Table Already Created")
except:
    jobdf.write.format("delta").mode("overwrite").save(auditlogpath+'/jobauditlog')
try:
    existingtaskdf = spark.read.load(auditlogpath+'/taskauditlog')
    print("Task Audit Table Already Created")
except:
    taskdf.write.format("delta").mode("overwrite").save(auditlogpath+'/taskauditlog')