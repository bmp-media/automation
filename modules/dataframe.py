import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows


class DataframeManager():

    @staticmethod
    def import_dataframe(file_path):
        return pd.read_csv(file_path, encoding='cp1251', sep='\t', header=None, low_memory=False)

    @ staticmethod
    def export_dataframe_to_sheet(dataframe, sheet):
        for row in dataframe_to_rows(dataframe, index=False, header=True):
            sheet.append(row)
