import pyspark.sql.types as T
from pyspark.sql.functions import col
import pandas as pd
import json

def sparkHeaderBuild(spark,dataDict_config_path,tableNm):

    dataDictDF = spark.read.option("multiline","true").json(path=dataDict_config_path)
    dataDictDF.createOrReplaceTempView("dataDict")
    schemaDF = spark.sql("select ColumnName , CAST(ColumnOrdinal AS INT) ORDINAL_POSITION from dataDict where TableName = '" +tableNm+"' order by ORDINAL_POSITION")
    schemaDF =schemaDF.sort(col("ORDINAL_POSITION"))
    ddl_schema = schemaDF.collect()
    ddl_schema_string = ''
    for s in ddl_schema:
            ddl_schema_string = ddl_schema_string +',' + s[0]

    dss = list(ddl_schema_string.lstrip(',').split(","))
    return dss