import pandas as pd
import numpy as np

def object_cols(df:pd.DataFrame):
    """
    Returns a list of column names in the given Dataframe that have the data type 'object'

    Parameters:
    df (pd.DataFrame): The input DataFrame

    """

    object_col_list = list(df.loc[:,df.dtypes == object].columns)
    return object_col_list

def int_cols(df:pd.DataFrame):
    """
    Returns a list of column names in the given Dataframe that have the data type 'int'

    Parameters:
    df (pd.DataFrame): The input DataFrame

    """

    int_col_list = list(df.loc[:,df.dtypes == int].columns)
    return int_col_list

def float_cols(df:pd.DataFrame):
    """
    Returns a list of column names in the given Dataframe that have the data type 'float'

    Parameters:
    df (pd.DataFrame): The input DataFrame

    """

    float_col_list = list(df.loc[:,df.dtypes == float].columns)
    return float_col_list

def numeric_cols(df:pd.DataFrame):
    """
    Returns a list of column names in the given Dataframe that have the data type 'int' or 'float'

    Parameters:
    df (pd.DataFrame): The input DataFrame

    """

    numeric_col_list = list(df.loc[:,((df.dtypes == float) | (df.dtypes == int))].columns)
    return numeric_col_list

def bool_cols(df:pd.DataFrame):
    """
    Returns a list of column names in the given Dataframe that have the data type 'bool'

    Parameters:
    df (pd.DataFrame): The input DataFrame

    """

    bool_col_list = list(df.loc[:,df.dtypes == bool].columns)
    return bool_col_list

def datetime_cols(df:pd.DataFrame):
    """
    Returns a list of column names in the given Dataframe that have the data type 'datetime'

    Parameters:
    df (pd.DataFrame): The input DataFrame

    """

    datetime_col_list = [col for col in df.columns if np.issubdtype(df[col].dtypes,np.datetime64)]
    return datetime_col_list

def cols_to_string(df: pd.DataFrame,list_of_columns: list):
    """
    Converts the data type of specified columns in the given DataFrame to 'string'.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    list_of_columns (list): A list of column names to be converted to dtype 'string'.

    Raises:
    ValueError: If any column in list_of_columns is not found in the DataFrame.
    Exception: If an error occurs during type conversion.
    """
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
    """
    Converts the data type of specified columns in the given DataFrame to 'float'.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    list_of_columns (list): A list of column names to be converted to dtype 'float'.

    Raises:
    ValueError: If any column in list_of_columns is not found in the DataFrame.
    Exception: If an error occurs during type conversion.
    """
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
    """
    Converts the data type of specified columns in the given DataFrame to 'int'.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    list_of_columns (list): A list of column names to be converted to dtype 'int'.

    Raises:
    ValueError: If any column in list_of_columns is not found in the DataFrame.
    Exception: If an error occurs during type conversion.
    """
    try:
        missing_cols = [col for col in list_of_columns if col not in df.columns]

        if missing_cols:
            raise ValueError(f'The following columns are not in the DF: {missing_cols}')
        
        for col in list_of_columns:
            df[col] = df[col].astype('Int64')
    
    except Exception as e:
        print(f'Failed to convert columns to dtype int: {e}')
        raise

def cols_to_bool(df: pd.DataFrame, list_of_columns: list):
    """
    Converts the data type of specified columns in the given DataFrame to 'bool'.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    list_of_columns (list): A list of column names to be converted to dtype 'bool'.

    Raises:
    ValueError: If any column in list_of_columns is not found in the DataFrame.
    Exception: If an error occurs during type conversion.
    """
    try:
        missing_cols = [col for col in list_of_columns if col not in df.columns]

        if missing_cols:
            raise ValueError(f'The following columns are not in the DF: {missing_cols}')
        
        for col in list_of_columns:
            df[col] = df[col].astype(bool)
    
    except Exception as e:
        print(f'Failed to convert columns to dtype boolean: {e}')
        raise

def cols_to_datetime(df: pd.DataFrame, list_of_columns: list):
    """
    Converts the data type of specified columns in the given DataFrame to 'datetime'.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    list_of_columns (list): A list of column names to be converted to dtype 'datetime'.

    Raises:
    ValueError: If any column in list_of_columns is not found in the DataFrame.
    Exception: If an error occurs during type conversion.
    """
    try:
        missing_cols = [col for col in list_of_columns if col not in df.columns]

        if missing_cols:
            raise ValueError(f'The following columns are not in the DF: {missing_cols}')
        
        for col in list_of_columns:
            df[col] = pd.to_datetime(df[col], errors = 'coerce')
    
    except Exception as e:
        print(f'Failed to convert columns to dtype datetime: {e}')
        raise
    
def cols_to_category(df: pd.DataFrame, list_of_columns: list):
    """
    Converts the data type of specified columns in the given DataFrame to 'category'.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    list_of_columns (list): A list of column names to be converted to dtype 'category'.

    Raises:
    ValueError: If any column in list_of_columns is not found in the DataFrame.
    Exception: If an error occurs during type conversion.
    """
    try:
        missing_cols = [col for col in list_of_columns if col not in df.columns]

        if missing_cols:
            raise ValueError(f'The following columns are not in the DataFrame: {missing_cols}')
        
        for col in list_of_columns:
            df[col] = df[col].astype('category')
    
    except Exception as e:
        print(f'Failed to convert columns to dtype category: {e}')
        raise

def dataframe_info(df: pd.DataFrame):
    """
    Generate a summary DataFrame containing metadata about the columns of the input DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: A summary DataFrame with the following columns:
            - Column_name: Name of each column.
            - Total records: Total number of records in each column.
            - Missing Values: Number of missing (NaN) values in each column.
            - Data type: Data type of each column.
            - Unique values: Number of unique values in each column.
    """
    df_summary = pd.DataFrame({
        'Column_name': df.columns,
        'Total records': [df[col].size for col in df.columns],
        'Missing Values': [df[col].isna().sum() for col in df.columns],
        'Data type': [df[col].dtype for col in df.columns],
        'Unique values': [df[col].nunique() for col in df.columns]
    })

    return df_summary

def value_counts_for_selected_columns(df: pd.DataFrame, columns: list):
    """
    Print value counts for the selected columns in the given DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        columns (list): List of column names for which value counts should be displayed.

    Returns:
        None
    """
    for col in columns:
        if col in df.columns:
            print(f'Value counts of {col}: \n{df[col].value_counts()}\n')
        else:
            print(f'Column "{col}" not found in DataFrame.\n')