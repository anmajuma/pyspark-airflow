import pyspark.sql.functions as F
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import *
import datetime

jobcolumns = StructType([StructField('JobID',
                                StringType(), True),
                    StructField('JobName',
                                  StringType(), True),
                    StructField('SourceSystemName',
                                StringType(), True),
                     StructField('SourceTableName',
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


def insertJobAuditData(spark,root,AuditItemId,AuditItemType,SourceSystemName,SourceTableName):
  # ct stores current time
  ct = datetime.datetime.now()
  if AuditItemType == 'JOB' :
    auditDeltaTbl = f"{root}/jobauditlog"
   
    data2 = [(AuditItemId,"daily_etl_job"+SourceSystemName+"_"+SourceTableName,SourceSystemName,SourceTableName,0,0,0,ct,ct,'Job Started','NA')]
    updates = spark.createDataFrame(data=data2,schema=jobcolumns)
    # updates.printSchema()
    # updates.show(truncate=False)
    dest = DeltaTable.forPath(spark, auditDeltaTbl)
    dest.alias("audit").merge(updates.alias("updates"),'audit.JobID = updates.JobID').whenNotMatchedInsertAll().execute()


def insertTaskAuditData(spark,root,JobID,AuditItemId,AuditItemType,TaskName,SourceSystemName,SourceTableName):
  # ct stores current time
  ct = datetime.datetime.now()
  if AuditItemType == 'TASK' :
    auditDeltaTbl = f"{root}/taskauditlog"  

    data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,0,0,'Task Started','NA')]
    updates = spark.createDataFrame(data=data2,schema=taskcolumns)
    # updates.printSchema()
    # updates.show(truncate=False)
    dest = DeltaTable.forPath(spark, auditDeltaTbl)
    dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenNotMatchedInsertAll().execute()


def updateTaskAuditDataPass(spark,root,JobID,AuditItemId,AuditItemType,TaskName,SourceSystemName,SourceTableName,row_count):
    # ct stores current time
  ct = datetime.datetime.now()
  if AuditItemType == 'TASK' :
    auditDeltaTbl = f"{root}/taskauditlog"  

    if TaskName == 'ingestSrcData':
      data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,row_count,0,0,'Task Completed','NA')]
      updates = spark.createDataFrame(data=data2,schema=taskcolumns)
      # updates.printSchema()
    # updates.show(truncate=False)
      dest = DeltaTable.forPath(spark, auditDeltaTbl)
      dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "RowsIngested": "updates.RowsIngested" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()
    if TaskName.__contains__('removeDuplicate'):
      data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,row_count,0,'Task Completed','NA')]
      updates = spark.createDataFrame(data=data2,schema=taskcolumns)
      # updates.printSchema()
    # updates.show(truncate=False)
      dest = DeltaTable.forPath(spark, auditDeltaTbl)
      dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "RowsRejected": "updates.RowsRejected" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()
    if TaskName.__contains__('schemaEnforcement'):
      data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,0,row_count,'Task Completed','NA')]
      updates = spark.createDataFrame(data=data2,schema=taskcolumns)
      # updates.printSchema()
    # updates.show(truncate=False)
      dest = DeltaTable.forPath(spark, auditDeltaTbl)
      dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "RowsUpserted": "updates.RowsUpserted" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()
    if TaskName.__contains__('applyDQRules'):
      data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,row_count,0,'Task Completed','NA')]
      updates = spark.createDataFrame(data=data2,schema=taskcolumns)
      # updates.printSchema()
    # updates.show(truncate=False)
      dest = DeltaTable.forPath(spark, auditDeltaTbl)
      dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "RowsRejected": "updates.RowsRejected" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()
    if TaskName == 'loadCleansedData_StageTable':
      data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,0,row_count,'Task Completed','NA')]
      updates = spark.createDataFrame(data=data2,schema=taskcolumns)
      # updates.printSchema()
    # updates.show(truncate=False)
      dest = DeltaTable.forPath(spark, auditDeltaTbl)
      dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "RowsUpserted": "updates.RowsUpserted" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()
    if TaskName == 'loadCleansedData_SilverTable':
      data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,0,row_count,'Task Completed','NA')]
      updates = spark.createDataFrame(data=data2,schema=taskcolumns)
      # updates.printSchema()
    # updates.show(truncate=False)
      dest = DeltaTable.forPath(spark, auditDeltaTbl)
      dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "RowsUpserted": "updates.RowsUpserted" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()


def updateTaskAuditDataFail(spark,root,JobID,AuditItemId,AuditItemType,TaskName,SourceSystemName,SourceTableName,error):
  # ct stores current time
  ct = datetime.datetime.now()
  if AuditItemType == 'TASK' :
    auditDeltaTbl = f"{root}/taskauditlog"  
    data2 = [(JobID,"etl_"+TaskName+"_"+SourceSystemName+"_"+SourceTableName,AuditItemId,ct,ct,0,0,0,'Task Failed',error)]
    updates = spark.createDataFrame(data=data2,schema=taskcolumns)
    dest = DeltaTable.forPath(spark, auditDeltaTbl)
    dest.alias("audit").merge(updates.alias("updates"),'audit.TaskID = updates.TaskID').whenMatchedUpdate(set = { "ErrorMessage": "updates.ErrorMessage" , "TaskStatus" : "updates.TaskStatus" , "TaskEndTime" :  "updates.TaskEndTime" }).execute()

