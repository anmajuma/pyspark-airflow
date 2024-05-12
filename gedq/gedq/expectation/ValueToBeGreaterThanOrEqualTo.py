from gedq.expectation.Expectation import Expectation
class ValueToBeGreaterThanOrEqualTo(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_be_between(column=self.column, 
                                                 min_value=self.add_info["min_value"],  
                                                 meta = {"dimension": self.dimension}, 
                                                 result_format="COMPLETE")