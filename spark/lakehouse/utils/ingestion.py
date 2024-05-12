import pymongo
import pandas as pd
from pandas import json_normalize
from pyspark.sql import functions as F
pd.DataFrame.iteritems = pd.DataFrame.items

def mongodb_data_ingestion(spark,srcconnoConfigPath,connnm,sourceName,tableNm,colNMs,jobID,landingPath):
    connDF = spark.read.option("multiline","true").json(path=srcconnoConfigPath)    
    mongoconnDF = connDF.filter("conntype == 'mongodb'").filter("connname =='"+connnm+"'")
    conninfoDF = mongoconnDF.select("conninfo")
    urlDF = conninfoDF.select("conninfo.url")
    url = urlDF.first()['url']
    userDF = conninfoDF.select("conninfo.user")
    user = userDF.first()['user']
    pwdDF = conninfoDF.select("conninfo.password")
    password = pwdDF.first()['password']
    srcnmDF = conninfoDF.select("conninfo.sourceName")
    sourceName = srcnmDF.first()['sourceName']
    mongo_con_url = "mongodb+srv://"+user+":"+password+"@"+url+"/"+sourceName+"?retryWrites=true&w=majority"
    # print(mongo_con_url)
    try:
        client=pymongo.MongoClient(mongo_con_url)
        mongodb=client[sourceName]
        collectionList = [tableNm]
        for x in collectionList:
            
            collectNm = mongodb[x]
            cursor = collectNm.find()
            pdf = json_normalize(cursor)
            pdf = pdf.drop(['_id'], axis=1)
            pdf1 = pd.DataFrame(pdf, columns=colNMs)
            sparkDF = spark.createDataFrame(pdf1)
            # sparkDF.show()
            df =sparkDF.withColumn('IngestTimeStamp', F.current_timestamp()).withColumn("batch_id",F.lit(jobID))
            landingPath = landingPath+"/mongodb/"+sourceName+"/"+tableNm+"/"
            df = df.withColumn("part_date", F.current_date())
            df.write.mode("overwrite").format("parquet").partitionBy("part_date").save(landingPath)
            return df.count()
    except Exception as Error:   
            return Error
    