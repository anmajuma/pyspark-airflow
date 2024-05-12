from gedq.expectation.Expectation import Expectation

class ValuesToMatchRegex(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_match_regex(column=self.column, 
                                                  regex =self.add_info["regex"], 
                                                  meta = {"dimension": self.dimension}, 
                                                  result_format="COMPLETE")