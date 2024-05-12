import json

class JSONFileReader:
    def __init__(self, filename,spark):
        self.filename = filename
        self.spark = spark

    def read(self):
        text = self.spark.sparkContext.wholeTextFiles(self.filename).first()[1]
        return text