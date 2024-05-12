from gedq.expectation.Expectation import Expectation

class ValueToBeOfType(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_be_of_type(column=self.column, 
                                                 type_=self.add_info["type"], 
                                                 meta = {"dimension": self.dimension}, 
                                                 result_format="COMPLETE")