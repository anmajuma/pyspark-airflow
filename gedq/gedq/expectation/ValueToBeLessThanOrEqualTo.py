from gedq.expectation.Expectation import Expectation

class ValueToBeLessThanOrEqualTo(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_be_between(column=self.column, 
                                                 max_value=self.add_info["max_value"],  
                                                 meta = {"dimension": self.dimension}, 
                                                 result_format="COMPLETE")