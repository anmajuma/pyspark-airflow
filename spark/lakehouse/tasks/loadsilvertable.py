from lakehouse.utils.jobTaskIDGen import genID
from lakehouse.utils.auditlog import insertTaskAuditData ,updateTaskAuditDataPass , updateTaskAuditDataFail
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql import functions as F
from delta.tables import *

def loadCleansedData_SilverTable(spark,jobID,stgpath,silverpath,tableNm,sourceName,sourceType,auditlogpath,dataDict_config_path,partition_columns):
    taskID = genID()
    print(taskID)
    TaskName = 'loadCleansedData_SilverTable'
    try:
        df =spark.read.format("delta").load(silverpath+"/"+sourceType+"/"+sourceName+"/"+tableNm).limit(1)
        flag = 1
    except:
        flag = 0   
    insertTaskAuditData(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm)
    dataDictDF = spark.read.option("multiline","true").json(path=dataDict_config_path)
    dataDictDF.createOrReplaceTempView("datadict")
    sourcePKDF= spark.sql("select ColumnName from datadict where ColumnPK = 'X' and TableName ='"+tableNm+"'")
    sourcePKrdd=sourcePKDF.collect()
    pkList = list()
    for x in sourcePKrdd:
        pkList.append(x[0])
    if len(pkList) > 0 :
        mergeClauseStr = ""
        for x in pkList:
            mergeClauseStr = mergeClauseStr + tableNm+"."+x + " = updates."+x + " and"
        mergerClause = mergeClauseStr.rstrip("and") 
        flg = 1   
    else:
        mergerClause = tableNm+".row_sha2 = updates.row_sha2"    
        flg =0
    print(flg,mergerClause)
    row_count = 0
    deltaDF = spark.read.format("delta").load(stgpath+"/"+sourceType+"/"+sourceName+"/"+tableNm)
    hashedDF = deltaDF.withColumn("row_sha2", sha2(concat_ws("||", *deltaDF.columns), 256))
    row_count = deltaDF.count()
    print(row_count)
    if row_count > 0 :
        if flg == 1 and flag == 1:
                print("Not First Load")
                deltaDF =deltaDF.withColumn("batchID",F.lit(jobID))
                deltaTableSilver = DeltaTable.forPath(spark, silverpath+"/"+sourceType+"/"+sourceName+"/"+tableNm)
                try:
                    deltaTableSilver.alias(tableNm) \
                    .merge(
                    deltaDF.alias('updates'),
                    mergerClause
                    ) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
                except Exception as error:
                    print(error)
                    updateTaskAuditDataFail(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)            
        elif flg == 1 and flag == 0:
                print("First Load")
                try:
                    deltaDF =deltaDF.withColumn("batchID",F.lit(jobID))
                    deltaDF.write.format("delta").partitionBy(partition_columns).save(silverpath+"/"+sourceType+"/"+sourceName+"/"+tableNm)
                except Exception as error:
                    print(error)
                    updateTaskAuditDataFail(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)        
        elif flg == 0 and flag == 1:
                print("Not First Load")
                hashedDF =deltaDF.withColumn("batchID",F.lit(jobID))
                deltaTableSilver = DeltaTable.forPath(spark, silverpath+"/"+sourceType+"/"+sourceName+"/"+tableNm)
                try:
                    deltaTableSilver.alias(tableNm) \
                    .merge(
                    hashedDF.alias('updates'),
                    mergerClause
                    ) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()       
                except Exception as error:
                    print(error)
                    updateTaskAuditDataFail(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)        
        elif flg == 0 and flag == 0:
                print("First Load")
                hashedDF =hashedDF.withColumn("batchID",F.lit(jobID))
                try:
                    hashedDF.write.format("delta").partitionBy(partition_columns).save(silverpath+"/"+sourceType+"/"+sourceName+"/"+tableNm)            
                except Exception as error:
                    print(error)
                    updateTaskAuditDataFail(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)                            
        updateTaskAuditDataPass(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,row_count)
    else:
        print("no new records")
        updateTaskAuditDataPass(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,row_count)
    auditDeltaTbl = f"{auditlogpath}/taskauditlog"              
    auditDF = spark.read.format("delta").load(auditDeltaTbl)
    return (auditDF.select("TaskStatus").filter("TaskID == '"+taskID+"'"))     