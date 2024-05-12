from gedq.expectation.Expectation import Expectation

class ValueLengthsToEqual(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_value_lengths_to_equal(column=self.column, 
                                                   value=self.add_info["value"], 
                                                   meta = {"dimension": self.dimension}, 
                                                   result_format="COMPLETE")