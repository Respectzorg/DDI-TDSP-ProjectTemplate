import configuration as config
import pandas as pd
from configuration import conn_engine


class QueryTable(object):
    def __init__(self, query, conn_engine=conn_engine):
        self.query = query
        self.dataframe = pd.read_sql(self.query, conn_engine)

    def replaceExactValueInColumn(self, from_value, to_value, column):
        mask = self.dataframe[column] == from_value
        self.dataframe.loc[mask, column] = to_value

    def replaceValueContainedInColumn(self, from_value, to_value, column):
        mask = self.dataframe[column].str.contains(from_value)
        self.dataframe.loc[mask, column] = to_value
