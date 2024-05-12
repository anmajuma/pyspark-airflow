from gedq.data_quality.DataQuality import DataQuality
from gedq.utils.utils import create_df_from_dq_results
from lakehouse.utils.schemaBuild import sparkSchemaBuild
from lakehouse.utils.jobTaskIDGen import genID
from lakehouse.utils.auditlog import insertTaskAuditData ,updateTaskAuditDataPass , updateTaskAuditDataFail
from pyspark.sql import functions as F
import json

def cleanBronzeData_applyDQRules(spark,jobID,brnzPath,errPath,tableNm,sourceName,sourceType,auditlogpath,dqconfigpath,dataDictConfigPath,dataDictMappingConfigPath):
    
    taskID = genID()
    print(taskID)
    TaskName = 'cleanBronzeData_applyDQRules'
    insertTaskAuditData(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm)
    dqconfigpath = dqconfigpath+"/"+sourceType+"/"+sourceName+"/"+tableNm+".txt"
    brnzPath = brnzPath+"/"+sourceType+"/"+sourceName+"/"+tableNm
    errPath = errPath + '/'+sourceType+'/'+sourceName+'/'+tableNm
    try:
        df=spark.read.format("delta").load(brnzPath)
        brnzDF = df.filter("batchID =='"+jobID+"'")
        dq = DataQuality(brnzDF, dqconfigpath,spark)
        dq_results = dq.run_test()
        dq_df = create_df_from_dq_results(spark, dq_results)
        dq_df.show(truncate=False)
        dq_df.createOrReplaceTempView("dqtbl")
        ddl_schema = sparkSchemaBuild(spark,dataDictConfigPath,dataDictMappingConfigPath,tableNm)
        schema_dict = json.loads(ddl_schema.json())
        colList = []
        for field in schema_dict["fields"]:
            colList.append(field['name'] + " " + field['type'])
        brnzDF.createOrReplaceTempView("brnztbl")
        rejectedRowCount = 0
        failedDF = spark.sql("select * from dqtbl where status = 'FAILED'")
        dqfailedCount = failedDF.count()
        if dqfailedCount > 0 :
            rows_looped = failedDF.select("column", "unexpected_values","dimension").collect()
            rejectedRowCount = 0
            for rows in rows_looped:
       
                print(rows[0])
                for col in colList:
                    if col.split(" ")[0] == rows[0]:
                        dtype = col.split(" ")[1]
                        break
                print(dtype)    
                listofvals = ""
                for val in rows[1]:
                    if dtype == "string":
                        listofvals = listofvals +"'"+ val + "',"
                    elif dtype == "integer":  
                        listofvals = listofvals + str(val) + ","
                print(listofvals.rstrip(","))
                errdf = spark.sql("select * from brnztbl where "+rows[0]+" in ("+listofvals.rstrip(",")+")")
                errdf=errdf.withColumn("error_desc",F.lit(rows[2]))
                errdf=errdf.withColumn("batchID",F.lit(jobID))
                errdf.write.mode("append").format("delta").save(errPath)
                rejectedRowCount = rejectedRowCount + errdf.count()
        updateTaskAuditDataPass(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,rejectedRowCount)    
    except Exception as error:
            print(error)
            updateTaskAuditDataFail(spark,auditlogpath,jobID,taskID,'TASK',TaskName,sourceType,tableNm,error)

    auditDeltaTbl = f"{auditlogpath}/taskauditlog"              
    auditDF = spark.read.format("delta").load(auditDeltaTbl)
    return (auditDF.select("TaskStatus").filter("TaskID == '"+taskID+"'"))    


