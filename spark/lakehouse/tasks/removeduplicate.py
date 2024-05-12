from lakehouse.utils.jobTaskIDGen import genID
from lakehouse.utils.deduplicate import deduplicate
from lakehouse.utils.auditlog import insertTaskAuditData ,updateTaskAuditDataPass , updateTaskAuditDataFail
from pyspark.sql import functions as F

def cleanRawData_removeDuplicate(spark,jobID,tempPath,landingPath,tableNm,sourceName,sourceType,auditlogpath,dataDict_config_path,partition_columns):

    taskID = genID()
    print(taskID)
    TaskName = 'cleanRawData_removeDuplicate'
    insertTaskAuditData(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm)
    rawDF = spark.read.format("parquet").load(landingPath+"/"+sourceType+"/"+sourceName+"/"+tableNm)
    ingestedRowCount = rawDF.count()
    
    try:
            deDuplicatedDF = deduplicate(spark,rawDF,dataDict_config_path,tableNm)
            cleanedRowCount = deDuplicatedDF.count()
            rejectedRowCount = ingestedRowCount - cleanedRowCount
            deDuplicatedDF.write.format("delta").mode("overwrite").partitionBy(partition_columns).save(tempPath+'/'+sourceType+'/'+sourceName+'/'+tableNm)           
            updateTaskAuditDataPass(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,rejectedRowCount)
            
    except Exception as error:
            print(error)
            updateTaskAuditDataFail(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)

    auditDeltaTbl = f"{auditlogpath}/taskauditlog"              
    auditDF = spark.read.format("delta").load(auditDeltaTbl)
    return (auditDF.select("TaskStatus").filter("TaskID == '"+taskID+"'"))            