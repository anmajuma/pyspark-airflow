from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType

def deduplicate(spark,rawDF,dataDict_config_path,tableName):

    dataDictDF = spark.read.option("multiline","true").json(path=dataDict_config_path)
    dataDictDF.createOrReplaceTempView("datadict")
    sourcePKDF= spark.sql("select ColumnName from datadict where ColumnPK = 'X' and TableName ='"+tableName+"'")
    sourcePKrdd=sourcePKDF.collect()
    pkList = list()
    for x in sourcePKrdd:
        pkList.append(x[0])        
    print(pkList)
    if len(pkList) == 0:

        try:
            hashedDF = rawDF.withColumn("row_sha2", sha2(concat_ws("||", *rawDF.columns), 256))
            deDuplicatedDF = hashedDF.dropDuplicates(["row_sha2"])
            return deDuplicatedDF
        except :
            #Defining the schema of the dataframe.
            schema = StructType([StructField("dummy", IntegerType(), True)])

            #Creating an empty dataframe.
            empty_df = spark.createDataFrame([], schema)
            return empty_df

    elif len(pkList) >= 1:

        try:
            deDuplicatedDF = rawDF.dropDuplicates(pkList)
            return deDuplicatedDF
        except :
            #Defining the schema of the dataframe.
            schema = StructType([StructField("dummy", IntegerType(), True)])
            #Creating an empty dataframe.
            empty_df = spark.createDataFrame([], schema)
            return empty_df
