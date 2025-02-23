import pandas as pd
import numpy as np

def object_cols(df:pd.DataFrame):

    object_col_list = list(df.loc[:,df.dtypes == object].columns)
    return object_col_list

def int_cols(df:pd.DataFrame):

    int_col_list = list(df.loc[:,df.dtypes == int].columns)
    return int_col_list

def float_cols(df:pd.DataFrame):

    float_col_list = list(df.loc[:,df.dtypes == float].columns)
    return float_col_list

def numeric_cols(df:pd.DataFrame):

    numeric_col_list = list(df.loc[:,((df.dtypes == float) | (df.dtypes == int))].columns)
    return numeric_col_list

def bool_cols(df:pd.DataFrame):

    bool_col_list = list(df.loc[:,df.dtypes == bool].columns)
    return bool_col_list

def datetime_cols(df:pd.DataFrame):

    datetime_col_list = [col for col in df.columns if np.issubdtype(df[col].dtypes,np.datetime64)]
    return datetime_col_list

def cols_to_string(df: pd.DataFrame,list_of_columns: list):

    try:
        missing_cols = [col for col in list_of_columns if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f' The following columns are not in the DF: {missing_cols}')
        for col in list_of_columns:
            df[col] = df[col].astype('string')

    except Exception as e:
        print(f'Failed to convert columns to dtype string: {e}')
        raise

def cols_to_float(df: pd.DataFrame,list_of_columns: list):

    try:
        missing_cols = [col for col in list_of_columns if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f' The following columns are not in the DF: {missing_cols}')
        for col in list_of_columns:
            df[col] = df[col].astype('float64')

    except Exception as e:
        print(f'Failed to convert columns to dtype float64: {e}')
        raise

def cols_to_int(df: pd.DataFrame, list_of_columns: list):

    try:
        missing_cols = [col for col in list_of_columns if col not in df.columns]

        if missing_cols:
            raise ValueError(f'The folllowing columns are not in the DF: {missing_cols}')
        
        for col in list_of_columns:
            df[col] = df[col].astype('Int64')
    
    except Exception as e:
        print(f'Failed to convert columns to dtype int: {e}')
        raise

def cols_to_bool(df: pd.DataFrame, list_of_columns: list):

    try:
        missing_cols = [col for col in list_of_columns if col not in df.columns]

        if missing_cols:
            raise ValueError(f'The folllowing columns are not in the DF: {missing_cols}')
        
        for col in list_of_columns:
            df[col] = df[col].astype('boolean')
    
    except Exception as e:
        print(f'Failed to convert columns to dtype boolean: {e}')
        raise

def cols_to_datetime(df: pd.DataFrame, list_of_columns: list):

    try:
        missing_cols = [col for col in list_of_columns if col not in df.columns]

        if missing_cols:
            raise ValueError(f'The folllowing columns are not in the DF: {missing_cols}')
        
        for col in list_of_columns:
            df[col] = pd.to_datetime(df[col], errors = 'coerce')
    
    except Exception as e:
        print(f'Failed to convert columns to dtype bool: {e}')
        raise

