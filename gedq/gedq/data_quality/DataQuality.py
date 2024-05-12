from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from gedq.reader.JSONFileReader import *
from gedq.expectation.NotNullExpectation import NotNullExpectation
from gedq.expectation.UniqueExpectation import UniqueExpectation
from gedq.expectation.ValuesInListExpectation import ValuesInListExpectation
from gedq.expectation.UniqueValuesCountToBeBetween import UniqueValuesCountToBeBetween
from gedq.expectation.ValueLengthsToBeBetween import ValueLengthsToBeBetween
from gedq.expectation.ValuesToMatchRegex import ValuesToMatchRegex
from gedq.expectation.ValueLengthsToEqual import ValueLengthsToEqual
from gedq.expectation.ValuesToMatchStrftimeFormat import ValuesToMatchStrftimeFormat
from gedq.expectation.ValuesToNotMatchRegex import ValuesToNotMatchRegex
from gedq.expectation.ValueToBeBetween import ValueToBeBetween
from gedq.expectation.ValueToBeGreaterThanOrEqualTo import ValueToBeGreaterThanOrEqualTo
from gedq.expectation.ValueToBeOfType import ValueToBeOfType

class DataQuality:

    def __init__(self, pyspark_df, config_path,spark):
        self.pyspark_df = pyspark_df
        self.config_path = config_path
        self.spark = spark

    def rule_mapping(self, dq_rule):
        return {
            "check_values_to_not_be_null": "NotNullExpectation",
            "check_values_to_be_unique": "UniqueExpectation",
            "check_values_to_be_in_set": "ValuesInListExpectation",
            "check_value_lengths_to_equal": "ValueLengthsToEqual",
            "check_value_lengths_to_be_between": "ValueLengthsToBeBetween",
            "check_values_to_be_between": "ValueToBeBetween",
            "check_values_to_be_greater_than": "ValueToBeGreaterThan",
            "check_values_to_be_greater_than_or_equal_to": "ValueToBeGreaterThanOrEqualTo",
            "check_values_to_be_less_than": "ValueToBeLessThan",
            "check_values_to_be_less_than_or_equal_to": "ValueToBeLessThanOrEqualTo",
            "check_values_to_be_of_type": "ValueToBeOfType",
            "check_values_to_match_regex": "ValuesToMatchRegex",
            "check_values_to_not_match_regex": "ValuesToNotMatchRegex",
            "check_values_to_match_strftime_format": "ValuesToMatchStrftimeFormat",
            "check_unique_value_count_to_be_between": "UniqueValuesCountToBeBetween"
        }[dq_rule]

    def _get_expectation(self):
        class_obj = globals()[self.rule_mapping()]
        return class_obj(self.extractor_args)
    
    def convert_to_ge_df(self):
        return SparkDFDataset(self.pyspark_df)
    
    def read_config(self):
        json_reader = JSONFileReader(self.config_path,self.spark)
        return json_reader.read()
      
    def run_test(self):
        ge_df = self.convert_to_ge_df()
        conf = self.read_config()
        confText = conf.replace("\\", "\\\\")
        config = json.loads(confText)
        print(config)
        for column in config["columns"]:
            if column["dq_rule(s)"] is None:
                continue
            for dq_rule in column["dq_rule(s)"]:
                expectation_obj = globals()[self.rule_mapping(dq_rule["rule_name"])]
                expectation_instance = expectation_obj(column["column_name"], dq_rule["rule_dimension"], dq_rule["add_info"])
                expectation_instance.test(ge_df)

        dq_results = ge_df.validate()
        return dq_results