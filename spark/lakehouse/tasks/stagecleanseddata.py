from lakehouse.utils.jobTaskIDGen import genID
from lakehouse.utils.headerBuild import sparkHeaderBuild
from lakehouse.utils.auditlog import insertTaskAuditData ,updateTaskAuditDataPass , updateTaskAuditDataFail
from delta.tables import *

def loadCleansedData_StageTable(spark,jobID,errpath,brnzpath,stgpath,silverpath,tableNm,sourceName,sourceType,auditlogpath,dataDict_config_path):
    taskID = genID()
    print(taskID)
    TaskName = 'loadCleansedData_StageTable'
    insertTaskAuditData(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm)
    colNMs = sparkHeaderBuild(spark,dataDict_config_path,tableNm)
    df = spark.read.format("delta").load(brnzpath+"/"+sourceType+"/"+sourceName+"/"+tableNm)
    errDF = spark.read.format("delta").load(errpath+'/'+sourceType+'/'+sourceName+'/'+tableNm)
    baddf = errDF.select(colNMs).filter("batchID =='"+jobID+"'")
    brnzDF = df.select(colNMs).filter("batchID =='"+jobID+"'")
    goodDF = brnzDF.exceptAll(baddf)
    try:
        tmpDF = spark.read.format("delta").load(silverpath+"/"+sourceType+"/"+sourceName+"/"+tableNm)
        silverDF = tmpDF.select(colNMs)
        deltaDF = goodDF.exceptAll(silverDF)
        row_count = deltaDF.count()
        print(row_count)
        try:
            deltaDF.write.format("delta").mode("overwrite").save(stgpath+"/"+sourceType+"/"+sourceName+"/"+tableNm)
            updateTaskAuditDataPass(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,row_count)
        except Exception as error:
            print(error)
            updateTaskAuditDataFail(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)    
    except:
        try:
            goodDF.write.format("delta").mode("overwrite").save(stgpath+"/"+sourceType+"/"+sourceName+"/"+tableNm)
            row_count = goodDF.count()
            print(row_count)
            updateTaskAuditDataPass(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,row_count)
        except Exception as error:
            print(error)
            updateTaskAuditDataFail(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)        
    auditDeltaTbl = f"{auditlogpath}/taskauditlog"              
    auditDF = spark.read.format("delta").load(auditDeltaTbl)
    return (auditDF.select("TaskStatus").filter("TaskID == '"+taskID+"'"))   