import pandas as pd
import numpy as np
pd.options.display.float_format = "{:,.2f}".format
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype


def profiling(dataframe, tablename, repoId, repo_scan_Id):
    output_df = pd.DataFrame()
    column_names = dataframe.columns

    for i in column_names:

        DataType = dataframe[i].dtype
        if DataType == "int64":
            DataType = 'Integer'
        elif DataType == "object":
            DataType = 'String'
        elif DataType == "float64":
            DataType = 'Float'
        elif DataType == 'datetime64[ns]':
            DataType = 'datetime'

        TotalRecordCount = dataframe[i].count()
        UniqueCount = dataframe[i].nunique()

        if is_numeric_dtype(dataframe[i]):
            MinLength = dataframe[i].astype(str)
            MinLength = MinLength.apply(len)
            MinLength = MinLength.min()
        elif is_string_dtype(dataframe[i]):
            MinLength = dataframe[i].astype(str)
            MinLength = MinLength.apply(len)
            MinLength = int(MinLength.min())

        if is_numeric_dtype(dataframe[i]):
            MaxLength = dataframe[i].astype(str)
            MaxLength = MaxLength.apply(len)
            MaxLength = int(MaxLength.max())
        elif is_string_dtype(dataframe[i]):
            MaxLength = dataframe[i].astype(str)
            MaxLength = MaxLength.apply(len)
            MaxLength = int(MaxLength.max())

        if is_numeric_dtype(dataframe[i]):
            Mean = dataframe[i].mean()
        else:
            Mean = np.nan

        if is_numeric_dtype(dataframe[i]):
            StdDev = dataframe[i].std()
        else:
            StdDev = np.nan

        if is_numeric_dtype(dataframe[i]):
            Min = dataframe[i].min()
        else:
            Min = np.nan

        if is_numeric_dtype(dataframe[i]):
            Max = dataframe[i].max()
        else:
            Max = np.nan

        if is_numeric_dtype(dataframe[i]):
            Percentaile_99 = dataframe[i].quantile(q=0.99)
        else:
            Percentaile_99 = np.nan

        if is_numeric_dtype(dataframe[i]):
            Percentaile_75 = dataframe[i].quantile(q=0.75)
        else:
            Percentaile_75 = np.nan

        if is_numeric_dtype(dataframe[i]):
            Percentaile_25 = dataframe[i].quantile(q=0.25)
        else:
            Percentaile_25 = np.nan

        if is_numeric_dtype(dataframe[i]):
            Percentaile_1 = dataframe[i].quantile(q=0.01)
        else:
            Percentaile_1 = np.nan

        output_df = output_df.append(
            {'RepositoryID': repoId, 'RepositoryScanID': repo_scan_Id, 'TableName': tablename,
             'ColumnName': i, 'DataType': DataType, 'TotalRecordCount': TotalRecordCount,
             'UniqueCount': UniqueCount,'MinLength': MinLength, 'MaxLength': MaxLength, 'Mean': Mean, 'StdDev': StdDev,
             'Min': Min, 'Max': Max, '99Percentaile': Percentaile_99,'75Percentile': Percentaile_75, '25percentile': Percentaile_25,
             '1Percentile': Percentaile_1}, ignore_index=True)

    return output_df

