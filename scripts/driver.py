import sys 
sys.path.append('/opt/bitnami/spark/python')
from lakehouse.utils import sparkSessionBuilder ,jobTaskIDGen , auditlog
from lakehouse.tasks import src2landing ,removeduplicate,schemaenforcement,dataqcheck,stagecleanseddata,loadsilvertable

spark = sparkSessionBuilder.createSparkSession()

jobID = jobTaskIDGen.genID()
print(jobID)

aws_bucket_name = "wba"
dataDictConfigPath = "s3a://%s/pipeline-config/schema/mongo/wwi/datadict.json" % aws_bucket_name
dataDictMappingConfigPath = "s3a://%s/pipeline-config/schema/mongo/mongoToSparkMapping.json" % aws_bucket_name
dqconfigpath = "s3a://%s/pipeline-config/dq" % aws_bucket_name
auditlogPath = "s3a://%s/auditlog" % aws_bucket_name
landingPath = "s3a://%s/landing" % aws_bucket_name
temppath = "s3a://%s/temp" % aws_bucket_name
brnzPath = "s3a://%s/bronze" % aws_bucket_name
errPath = "s3a://%s/error" % aws_bucket_name
stgpath = "s3a://%s/stage" % aws_bucket_name
silverpath = "s3a://%s/silver" % aws_bucket_name
sourceConnName = "wwi"
sourceType = "mongodb"
sourceName = "WideWorldImporters"
tablenm = 'Colors'
partition_columns = ["ValidFrom"]

taskStatusDF = removeduplicate.cleanRawData_removeDuplicate(spark,jobID,temppath,landingPath,tablenm,sourceName,sourceType,auditlogPath,dataDictConfigPath,partition_columns)
taskStatusDF.show()
TaskStatus = taskStatusDF.first()['TaskStatus']
print(TaskStatus)