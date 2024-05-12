from lakehouse.utils import jobTaskIDGen , auditlog, headerBuild
from lakehouse.utils.ingestion import mongodb_data_ingestion

def ingestSrcData(spark,jobID,dataDictConfigPath,srcConfigPath,tableNm,sourceName,sourceType,connnm,root,landingPath):

    taskID = jobTaskIDGen.genID()
    print(taskID)
    TaskName = 'ingestSrcData'
    auditlog.insertTaskAuditData(spark,root,jobID,taskID,'TASK',TaskName,sourceType,tableNm)
    colNMs = headerBuild.sparkHeaderBuild(spark,dataDictConfigPath,tableNm)
    ingestion_func = globals()[sourceType+"_data_ingestion"]
    tmp = 0
    try:
        ingestionCount = ingestion_func(spark,srcConfigPath,connnm,sourceName,tableNm,colNMs,jobID,landingPath)
        try:
            tmp = int(ingestionCount)
            auditlog.updateTaskAuditDataPass(spark,root,jobID,taskID,'TASK',TaskName,sourceType,tableNm,ingestionCount)
        except Exception as error:
            auditlog.updateTaskAuditDataFail(spark,root,jobID,taskID,'TASK',TaskName,sourceType,tableNm,tmp)
           
    # Do something
    except Exception as error:
            auditlog.updateTaskAuditDataFail(spark,root,jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)
            # print(error)
    auditDeltaTbl = f"{root}/taskauditlog"              
    auditDF = spark.read.format("delta").load(auditDeltaTbl)
    return (auditDF.select("TaskStatus").filter("TaskID == taskID"))
            