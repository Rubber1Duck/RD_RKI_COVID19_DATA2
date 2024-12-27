import os
import pandas as pd
from pandas.api.types import CategoricalDtype


def squeeze_dataframe(df):
    # ----- Get columns in dataframe
    cols = dict(df.dtypes)

    # ----- Check each column's type downcast or categorize as appropriate
    for col, type in cols.items():
        if type == "float64":
            df[col] = pd.to_numeric(df[col], downcast="float")
        elif type == "int64":
            df[col] = pd.to_numeric(df[col], downcast="integer")
        elif type == "object":
            df[col] = df[col].astype(CategoricalDtype(ordered=True))

    return df


def write_file(df, fn, compression=""):
    fn_ext = os.path.splitext(fn)[1]

    if fn_ext == ".csv":
        df.to_csv(fn, index=False)

    elif fn_ext == ".feather":
        compression = "zstd" if compression == "" else compression
        df.to_feather(fn, compression=compression)

    else:
        print("oopsy in write_file()! File extension unknown:", fn_ext)
        quit(0)

    return


def write_json(df, full_fn):
    df.to_json(
        path_or_buf=full_fn,
        orient="records",
        date_format="iso",
        force_ascii=False,
        compression="infer",
    )

    return


def read_json(full_fn, dtype):
    df = pd.read_json(full_fn, dtype=dtype)

    return df


def read_file(fn):
    fn_ext = os.path.splitext(fn)[1]

    if fn_ext == ".csv":
        df = pd.read_csv(fn, keep_default_na=False)

    elif fn_ext == ".feather":
        df = pd.read_feather(fn)

    else:
        print("oopsy in read_file()! File extension unknown:", fn_ext)
        quit(0)

    return df


def calc_incidence(df):
    Indexes = df.index.to_list()
    df_cases_values = df["c"].values
    y = 0
    for index in Indexes:
        indexPos = Indexes.index(index)
        df.at[index, "c7"] = sum(df_cases_values[indexPos - y:indexPos + 1])
        if y < 6: y += 1
    df["c7"] = df["c7"].astype(int)
    return df

def copy(source, destination):
   with open(source, 'rb') as file:
       myFile = file.read()
   with open(destination, 'wb') as file:
       file.write(myFile)

def get_different_rows(source_df, new_df):
    """Returns just the rows from the new dataframe that differ from the source dataframe"""
    merged_df = source_df.merge(new_df, indicator=True, how='outer')
    changed_rows_df = merged_df[merged_df['_merge'] == 'right_only']
    return changed_rows_df.drop('_merge', axis=1)