import pyspark.sql.functions as F
from pyspark.sql.functions import col
import pandas as pd

def dataFormatStandardize(spark,brnzDF,ddl_schema ) :
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  schemaList = []
  brnzDF = brnzDF.na.fill(value=0)
  brnzDF = brnzDF.na.fill("")  
# Get the column names and types
  for field in ddl_schema.fields:
    schemaDict ={}
    schemaDict["fieldname"] = field.name
    schemaDict["DATA_TYPE"] = field.dataType
    schemaList.append(schemaDict)

  schemadf = pd.DataFrame(schemaList)
  if len(schemadf) > 0 :
    for index, row in schemadf.iterrows():
      if row["DATA_TYPE"] == "DateType" or row["DATA_TYPE"] == "TimestampType":
          print(row["fieldname"])
          bdf1=brnzDF.withColumn(row["fieldname"],F.to_timestamp(col(row["fieldname"])))
          bdf2 = bdf1.withColumn(row["fieldname"],F.date_format(col(row["fieldname"]),"MM-dd-YYYY HH:mm:ss"))
          brnzDF= bdf2
      else:
          bdf2=brnzDF.withColumn(row["fieldname"],(col(row["fieldname"])).cast(row["DATA_TYPE"]))
          brnzDF= bdf2              

    return (bdf2)
  else:
    return (brnzDF)