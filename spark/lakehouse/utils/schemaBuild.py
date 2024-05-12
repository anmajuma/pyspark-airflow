import pyspark.sql.types as T
from pyspark.sql.functions import col
import pandas as pd
import json

def sparkSchemaBuild(spark,dataDict_config_path,data_dict_mapping_config_path,tableNm):

    dataDictDF = spark.read.option("multiline","true").json(path=dataDict_config_path)
    pdf = pd.DataFrame()
    dataDictMappingDF = spark.read.option("multiline","true").json(path=data_dict_mapping_config_path)
    sourceColType = []
    targetColType = []
    for colnm in dataDictMappingDF.columns:
        sourceColType.append(colnm)
        tgtColDF = dataDictMappingDF.select(colnm)
        tgtColrdd=tgtColDF.collect()
        for x in tgtColrdd:
            targetColType.append(x[0])

    data = {'sourceColType': sourceColType,
        'targetColType': targetColType}
# Convert the dictionary into DataFrame
    pdf = pd.DataFrame(data)
    datatypeDF=spark.createDataFrame(pdf)
    dataDictDF.createOrReplaceTempView("dataDict")
    datatypeDF.createOrReplaceTempView("dataType")
    schemaDF = spark.sql("select dataDict.ColumnName || ' ' || dataType.targetColType  ddl_schema_string ,  CAST(ColumnOrdinal AS INT) ORDINAL_POSITION from dataDict JOIN dataType ON dataDict.ColumnDataType = dataType.sourceColType where  dataDict.TableName = '" +tableNm+"'")
    schemaDF =schemaDF.sort(col("ORDINAL_POSITION"))
    ddl_schema = schemaDF.collect()
    ddl_schema_string = ''
    for x in ddl_schema:
        ddl_schema_string = ddl_schema_string + ',' + x[0]
    ddl_schema_string = ddl_schema_string.lstrip(',')
    ddl_schema = T._parse_datatype_string(ddl_schema_string.lstrip(','))
    return ddl_schema
    # ddl_header = list(ddl_schema.split(","))
