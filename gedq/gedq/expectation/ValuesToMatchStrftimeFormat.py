from gedq.expectation.Expectation import Expectation

class ValuesToMatchStrftimeFormat(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_values_to_match_strftime_format(column=self.column,
                                                            strftime_format=self.add_info["format"],
                                                            meta = {"dimension": self.dimension},
                                                            result_format="COMPLETE")