from lakehouse.utils.jobTaskIDGen import genID
from lakehouse.utils.dataformater import dataFormatStandardize
from lakehouse.utils.schemaBuild import sparkSchemaBuild
from lakehouse.utils.auditlog import insertTaskAuditData ,updateTaskAuditDataPass , updateTaskAuditDataFail
from pyspark.sql import functions as F

def cleanRawData_schemaEnforcement(spark,jobID,brnzPath,tempPath,tableNm,sourceName,sourceType,root,dataDict_config_path,dataDictMappingConfigPath,partition_columns):

    deDuplicatedDF= spark.read.format("delta").load(tempPath+'/'+sourceType+'/'+sourceName+'/'+tableNm)           
    ddl_schema = sparkSchemaBuild(spark,dataDict_config_path,dataDictMappingConfigPath,tableNm)
    taskID = genID()
    print(taskID)
    TaskName = 'cleanRawData_schemaEnforcement'
    insertTaskAuditData(spark,root,jobID,taskID,'TASK',TaskName,sourceType,tableNm)
    try:
        brnzDF = dataFormatStandardize(spark,deDuplicatedDF,ddl_schema)
        brnzDF = brnzDF.withColumn("batchID",F.lit(jobID))
        row_count = brnzDF.count()
        brnzDF.write.format("delta").mode("append").partitionBy(partition_columns).save(brnzPath+'/'+sourceType+'/'+sourceName+'/'+tableNm)           
        updateTaskAuditDataPass(spark,root,jobID,taskID,'TASK',TaskName,sourceType,tableNm,row_count)
    except Exception as error:
            print(error)
            updateTaskAuditDataFail(spark,root,jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)
    auditDeltaTbl = f"{root}/taskauditlog"              
    auditDF = spark.read.format("delta").load(auditDeltaTbl)
    return (auditDF.select("TaskStatus").filter("TaskID == '"+taskID+"'"))            