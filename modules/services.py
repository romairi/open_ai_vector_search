import glob
import shutil
import re
import datetime as dt
import time
import json
import pandas as pd
import numpy as np
from configparser import ConfigParser
from pathlib import Path
from subprocess import Popen
import warnings
from typing import Dict, List

warnings.simplefilter("ignore")


def get_config(config_path: str = r'../config/config_safe.ini') -> ConfigParser:
    # todo: add a check statement if file exist
    config = ConfigParser()
    config.read(Path(__file__).parent / config_path)
    return config


def get_json(json_path: str = r'../config/config_values.json',
             encoding_type: str = 'utf-8'):
    path_to_json = Path(__file__).parent / json_path
    with open(path_to_json, encoding=encoding_type) as json_file:
        json_data = json.load(json_file)
    return json_data


def get_config_values_json(json_section: str = 'paths') -> List[str]:
    """
    This function gets the variables for the paths needed from the json config file
    :param json_section: the actual section needed in the Json file
    :return: list of the paths variables needed
    """
    json_data = get_json()  # r'../config/config_values.json'
    get_paths = json_data[json_section]
    safe_path, datalake_path, cleared_path, log_path, incorrect_version = tuple(get_paths)
    return get_paths


def get_config_values_ini(ini_section_urls: str = 'urls',
                          safe_path: int = 0,
                          datalake_path: int = 1,
                          cleared_path: int = 2,
                          log_path: int = 3,
                          log_incorrect_version: int = 4
                          ) -> List[str]:
    """
    This function stores in a list the paths needed from the config ini file
    :param ini_section_urls: the sub-section in the config ini file
    :param safe_path: the path to the safes where the inspectors files are located
    :param datalake_path: the drop path to the datalake from which the files would be loaded to the DL
    :param cleared_path: the archive path where the older inspectors files (that were already loaded to the DL)
    would be saved for a period of 1 month
    :param log_path: the path where a log of the loads would be saved
    :param log_incorrect_version: the path where a log of the incorrect versions were sent
    :return: list of the paths needed from the config ini file
    """
    config = get_config()  # r'../config/config_safe.ini'
    paths_json = get_config_values_json()
    path_to_safe = config.get(ini_section_urls, paths_json[safe_path])
    datalake_drop_path = config.get(ini_section_urls, paths_json[datalake_path])
    cleared_folder = config.get(ini_section_urls, paths_json[cleared_path])
    export_log_path = config.get(ini_section_urls, paths_json[log_path])
    export_incorrect_versions = config.get(ini_section_urls, paths_json[log_incorrect_version])
    paths_needed = []
    paths_needed.extend((path_to_safe, datalake_drop_path, cleared_folder, export_log_path, export_incorrect_versions))
    return paths_needed


def list_sub_folders(dir_path: str) -> List[str]:
    """
    This function builds a list that contains the paths of all the sub-folders
    :param dir_path: the main directory where all the sub folders are located
    :return: a list with the paths of all the sub-folders
    """
    # todo: check if the dir path exists and it's a directory
    sub_folders = glob.glob(dir_path, recursive=True)
    print(sub_folders)
    if not sub_folders:
        raise Exception("no sub folders to read from")
    return sub_folders


def full_path_files(sub_folders_paths: List[str],
                    file_type: str = '/*.xlsm') -> List[List[str]]:
    """
    This function creates a list of lists that contains the full paths of the files
    that are stored in the sub folders
    :param sub_folders_paths: list that contains the paths of the sub folders
    :param file_type: the type of file
    :return: list of lists that with the full paths of the files in the sub folders
    """
    full_paths_list = []
    for i in sub_folders_paths:
        full_paths = glob.glob(f'{i}{file_type}')
        full_paths_list.append(full_paths)
    return full_paths_list


def flatten_list_of_lists(unflatten_list: List[List[str]]) -> List[str]:
    """
    This function flatten the list of lists into a list
    :param unflatten_list: list of lists with the path of the files
    :return: a flatten list with the paths of the files
    """
    flatten_list = [i for s in unflatten_list for i in s]
    return flatten_list


def filter_files_regex(flatten_list: List[str],
                       file_pattern: str = r".*[Pp]_[0-9]+_[0-9]{6}\.xlsm$") -> List[str]:
    """
    This function keeps only the file names that follow a specific regex pattern
    :param flatten_list: list that contains the paths of the files
    :param file_pattern: specific regex pattern that the file names need to follow
    :return: list that contains the full paths of the files that match a specific regex pattern
    """
    r = re.compile(file_pattern)
    keep_files = list(filter(r.match, flatten_list))
    print(keep_files)
    return keep_files


def list_of_dfs(final_flatten_list: List[str]) -> List[pd.DataFrame]:
    """
    This function reads all Excel files, and then saves them as a
    list of DataFrames
    :param final_flatten_list: list that contains the paths of the Excel files
    :return: list of DataFrames
    """
    list_df = []
    for i in final_flatten_list:
        try:
            df = pd.read_excel(i, header=None, engine='openpyxl')
            list_df.append(df)
        except:
            print("wrong file format")
    return list_df


def dict_paths_and_dfs(keep_needed_files: List[str],
                       list_of_dfs: List[pd.DataFrame]):
    """
    This function creates a dictionary where the keys are the files paths and the values are the dfs. This is needed in order to
    eventually filter the file paths where the version number is older. The version number is taken from the df
    :param keep_needed_files: the paths of the files
    :param list_of_dfs: the dataframes of the files loaded
    :return: Dictionary where the keys are the files paths and the values are the dfs
    """
    dict_path_df = {}
    for path, df in zip(keep_needed_files, list_of_dfs):
        dict_path_df[path] = df
    return dict_path_df


def version_control_dfs(list_df: List[pd.DataFrame],
                        row_num: int = 0,
                        column_num: int = 6,
                        version_num: str = 'V2_0_1') -> List[pd.DataFrame]:
    """
    This function keeps a List of DataFrames for a specific version located in cell G1 in the Excel files
    :param list_df: list of DataFrames
    :param row_num: the row in the Excel file where the version number is located
    :param column_num: the column in the Excel file where the version number is located
    :param version_num: the version number needed
    :return: A List of DataFrames for a specific version needed
    """
    latest_version_list_df = []
    for df in list_df:
        if df.iloc[row_num, column_num] == version_num:
            latest_version_list_df.append(df)
    return latest_version_list_df


def version_control_files(dict_path_df,
                          row_num: int = 0,
                          column_num: int = 6,
                          version_num: str = 'V2_0_1'
                          ) -> List[str]:
    """
    This function creates list of the paths of all the files needed based on the latest version defined
    :param dict_path_df: Dictionary where the keys are the files paths and the values are the dfs
    :param row_num: the row in the Excel file where the version number is located
    :param column_num: the column in the Excel file where the version number is located
    :param version_num: the version number needed
    :return: list of the paths of all the files needed based on the latest version defined
    """
    latest_version_list_files = []
    for path, df in dict_path_df.items():
        if df.iloc[row_num, column_num] == version_num:
            latest_version_list_files.append(path)
    return latest_version_list_files


def diff_version_files_checks(dict_path_df,
                              row_num: int = 0,
                              column_num: int = 6,
                              version_num: str = 'V2_0_1'
                              ) -> List[str]:
    """
    This function creates list of the paths of all the files that were excluded (not loaded to the DL)
    because they are of a different version that the one defined
    :param dict_path_df: Dictionary where the keys are the files paths and the values are the dfs
    :param row_num: the row in the Excel file where the version number is located
    :param column_num: the column in the Excel file where the version number is located
    :param version_num: the version number needed
    :return: list of the paths of all the files not loaded because of incorrect version
    """
    diff_version_list_files = []
    for path, df in dict_path_df.items():
        if df.iloc[row_num, column_num] != version_num:
            diff_version_list_files.append(path)
    return diff_version_list_files


def diff_version_inspectors(dict_path_df,
                            row_num: int = 0,
                            column_num: int = 6,
                            version_num: str = 'V2_0_1',
                            inspector_row_num: int = 2,
                            inspector_col_num: int = 2
                            ) -> List[str]:
    """
    This function creates a list of the inspectors names for all the files that were excluded (not loaded to the DL)
    because they are of a different version that the one defined
    :param dict_path_df: Dictionary where the keys are the files paths and the values are the dfs
    :param row_num: the row in the Excel file where the version number is located
    :param column_num: the column in the Excel file where the version number is located
    :param version_num: the version number needed
    :param inspector_row_num: the row in the Excel file where the inspector name is located
    :param inspector_col_num: the column in the Excel file where the inspector name is located
    :return: list of the inspectors names for all the files not loaded because of incorrect version
    """
    diff_version_inspectors = []
    for path, df in dict_path_df.items():
        if df.iloc[row_num, column_num] != version_num:
            inspectors_names = df.iloc[inspector_row_num, inspector_col_num]
            diff_version_inspectors.append(inspectors_names)
    return diff_version_inspectors


def diff_version_report_date(dict_path_df,
                             row_num: int = 0,
                             column_num: int = 6,
                             version_num: str = 'V2_0_1',
                             date_row_num: int = 4,
                             date_col_num: int = 2
                             ) -> List[str]:
    """
    This function creates a list of the report dates for all the files that were excluded (not loaded to the DL)
    because they are of a different version that the one defined
    :param dict_path_df: Dictionary where the keys are the files paths and the values are the dfs
    :param row_num: the row in the Excel file where the version number is located
    :param column_num: the column in the Excel file where the version number is located
    :param version_num: the version number needed
    :param date_row_num: the row in the Excel file where the report date is located
    :param date_col_num: the column in the Excel file where the report date is located
    :return: list of the report dates for all the files not loaded because of incorrect version
    """
    diff_version_dates = []
    for path, df in dict_path_df.items():
        if df.iloc[row_num, column_num] != version_num:
            current_month_date = df.iloc[date_row_num, date_col_num]
            diff_version_dates.append(current_month_date)
    return diff_version_dates


def convert_datetime_obj_to_date_str(diff_version_dates,
                                     date_format: str = '%Y-%m-%d') -> List[str]:
    """
    This function converts the datetime.datetime object to a string object and stores
    it in a list (these dates are the report dates of the incorrect versions that were
    not loaded to the DL)
    :param diff_version_dates: the original list with the datetime.datetime object
    :param date_format: the required date format needed
    :return: list of dates (converted from datetime.datetime to date)
    """
    converted_diff_version_dates = []
    for i in diff_version_dates:
        timestamp = i.strftime(date_format)
        converted_diff_version_dates.append(timestamp)
    return converted_diff_version_dates


def convert_list_to_df(diff_version_list_files: List[str]) -> pd.DataFrame:
    """
    This function converts a list to df, to be exported as a new log of the files that were excluded
    due to differnt versions
    :param diff_version_list_files
    :return: DataFrame with the file paths of the files that were excluded
    """
    df = pd.DataFrame(diff_version_list_files)
    return df


def clean_excel_files(list_df: List[pd.DataFrame],
                      json_data,
                      json_section: str = 'clean_excel_files') -> List[pd.DataFrame]:
    """
    This function replaces an area, based on a range of cells, from the previous values
    that are not needed, into null values
    :param list_df: list of DataFrames that contain the data from the Excel files
    :param json_data: contains the constants values needed in a form of a dictionary
    :param json_section: the actual section needed in the Json file
    :return: list of DataFrame where the previous not needed values were replaces with nulls
    """
    remove_chr = json_data[json_section]
    final_list = []
    start_row, end_row, start_column, end_column = remove_chr
    for df in list_df:
        df.iloc[start_row:end_row, start_column:end_column] = np.nan
        final_list.append(df)
    return final_list


def transpose_dfs(final_list_df: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """
    This function transposes all the DataFrames to create a tabular form
    :param final_list_df: list of DataFrames
    :return: list of DataFrames that are now transposed
    """
    transpose_all_dfs = []
    for df in final_list_df:
        df_transposed = df.transpose()
        transpose_all_dfs.append(df_transposed)
    return transpose_all_dfs


def fill_first_columns(all_dfs: List[pd.DataFrame],
                       num_columns: int = 40,
                       src_col_value: int = 2) -> List[pd.DataFrame]:
    """
    This function fills empty fields with the value located in row with index 2, across the first 40 columns
    :param all_dfs: list of DataFrames
    :param num_columns: the number of columns that needs to be filled
    :param src_col_value: the index of the row where the original information is located
    :return: list of DataFrames, where the first 40 columns are now filled with the needed information
    from the rows
    """
    fill_columns = []
    for df in all_dfs:
        for column in range(num_columns):  # Iterate via a generator of 40 numbers
            # Fill empty fields with the value located in row with index 2, across the first 40 columns
            df[column] = df[column].fillna(df.iloc[src_col_value][column])
        fill_columns.append(df)
    return fill_columns


def fill_totals_columns(fill_col: List[pd.DataFrame],
                        json_data,
                        json_section: str = 'fill_totals_columns',
                        col_value: int = 0,
                        row_value: int = 1) -> List[pd.DataFrame]:
    """
    This function fills the total columns with the same values (the data is coming from L45 to L60 non-sequential)
    :param fill_col: list of DataFrames
    :param json_data: contains the constants values needed in a form of a dictionary
    :param json_section: the actual section needed in the Json file
    :param col_value: the index value of the column where the original data is located in the Excel file
    :param row_value: the index value of the row where the original data is located in the Excel file
    :return: list of DataFrames, where the total columns are added at the end
    """
    fill_totals = json_data[json_section]
    fill_totals_cols = []
    for df in fill_col:
        for key, value in fill_totals.items():
            df[key] = df.iloc[value[col_value], value[row_value]]
        fill_totals_cols.append(df)
    return fill_totals_cols


def drop_columns(fill_columns: List[pd.DataFrame],
                 json_data,
                 json_section: str = 'drop_columns',
                 axis_num: int = 1) -> List[pd.DataFrame]:
    """
    This function drops columns that are not needed from the DataFrames
    :param fill_columns: list of DataFrames
    :param json_data: contains the constants values needed in a form of a dictionary
    :param json_section: the actual section needed in the Json file
    :param axis_num: value of axis 1 which represents columns in the DataFrame
    :return: list of DataFrames after the drop of columns that are not needed
    """
    columns_to_drop = json_data[json_section]
    clean_df_after_dropping_columns = []
    for df in fill_columns:
        df = df.drop(df.columns[columns_to_drop], axis=axis_num)
        clean_df_after_dropping_columns.append(df)
    return clean_df_after_dropping_columns


def add_new_empty_columns(clean_df_after_drop: List[pd.DataFrame],
                          json_data,
                          json_section: str = 'add_new_empty_columns',
                          col_position: int = 0,
                          col_name: int = 1,
                          col_empty: int = 2) -> List[pd.DataFrame]:
    """
    This function add new empty columns to the DataFrames where eventually new data will be entered
    :param clean_df_after_drop: list of DataFrames
    :param json_data: contains the constants values needed in a form of a dictionary
    :param json_section: the actual section needed in the Json file
    :param col_position: the index value of the new columns to be added
    :param col_name: the name of the columns to be added
    :param col_empty: the empty value that the columns would have initially
    :return: list of DataFrames with the new empty columns
    """
    columns_to_add = json_data[json_section]
    add_new_columns = []
    for df in clean_df_after_drop:
        for value in columns_to_add.values():
            df.insert(value[col_position], value[col_name], value[col_empty])
        add_new_columns.append(df)
    return add_new_columns


def add_load_time(clean_after_adding_columns: List[pd.DataFrame],
                  col_name: str = 'load_time') -> List[pd.DataFrame]:
    """
    This function adds a timestamp under the new load_time column
    :param clean_after_adding_columns: list of DataFrames
    :param col_name: is the column name for the column with the new timestamp
    :return: list of DataFrames with a timestamp under the new load_time column
    """
    add_time = []
    for df in clean_after_adding_columns:
        df[col_name] = dt.datetime.now()
        add_time.append(df)
    return add_time


def remove_top_rows(clean_after_adding_time: List[pd.DataFrame],
                    number_of_rows_to_remove: int = 3) -> List[pd.DataFrame]:
    """
    This function removes the top 3 rows which are not needed (those rows contains the headings in hebrew)
    :param clean_after_adding_time: list of DataFrames
    :param number_of_rows_to_remove: the number of top rows that need to be removed
    :return: list of DataFrames without the original top 3 rows
    """
    remove_the_top_rows = []
    for df in clean_after_adding_time:
        df = df.iloc[number_of_rows_to_remove:]
        remove_the_top_rows.append(df)
    return remove_the_top_rows


def replace_dates_with_empty(clean_after_top_rows_removed: List[pd.DataFrame],
                             col_num: int = 40,
                             len_used: int = 3,
                             empty_value: str = '') -> List[pd.DataFrame]:
    """
    This function replaces cases where someone placed date with 8 characters instead of integers. If the number of characters
    is greater than 3, then replace that value in the cell into an empty value
    :param clean_after_top_rows_removed: list of DataFrames
    :param col_num: the location of the desired column, where the original data is located
    :param len_used: number of characters (here 3) in which any value above the specified value
    would be replaced with empty value
    :param empty_value: an empty string
    :return: list of DataFrames, where a value that has more than 3 characters, would be replaced with an empty value
    """
    replace_dates = []
    for df in clean_after_top_rows_removed:
        df.iloc[:, col_num].loc[df.iloc[:, col_num].astype(str).map(len) > len_used] = empty_value
        replace_dates.append(df)
    return replace_dates


def clean_dataframes(fill_columns: List[pd.DataFrame],
                     json_data,
                     json_section: str = 'clean_dataframes',
                     double_quotes: str = r'"',
                     empty_value: str = '') -> List[pd.DataFrame]:
    """ This function cleans the DataFrames by replacing all instances where quotes (or ;) are used into empty values
    :param fill_columns: list of DataFrames
    :param json_data: contains the constants values needed in a form of a dictionary
    :param json_section: the actual section needed in the Json file
    :param double_quotes: represents double quotes to be replaced with an empty value
    :param empty_value: the actual empty value/string, where the previous values would be replaced into
    :return: list of DataFrames after replacing all instances where quotes (or ;) were used
    """
    chr_to_replace = json_data[json_section]
    chr_to_replace.append(double_quotes)
    cleaned_dfs = []
    for df in fill_columns:
        df = df.replace(chr_to_replace, empty_value, regex=True)
        cleaned_dfs.append(df)
    return cleaned_dfs


def fix_dates(cleaned_dfs_before_fixing_dates: List[pd.DataFrame],
              json_data,
              json_section: str = 'fix_dates',
              json_additional_section: str = 'fix_dates_old_date',
              error_type: str = 'coerce',
              unit_type: str = 'd',
              src_col: int = 0,
              old_year: int = 1,
              target_col: int = 2
              ) -> List[pd.DataFrame]:
    """
    This function fixes the dates in particular columns, where the dates were inserted as numbers (starting with 44)
    in the original Excel files
    :param cleaned_dfs_before_fixing_dates: list of DataFrames
    :param json_data: contains the constants values needed in a form of a dictionary
    :param json_section: the actual section needed in the Json file
    :param json_additional_section: an additional section needed in the Json file
    :param error_type: set to 'coerce' to avoid errors when using to_datetime and TimedeltaIndex
    :param unit_type: set to 'd' to convert 1970 to date
    :param src_col: the original columns where the dates need to be fixed
    :param old_year: used in order to convert the original dates into the conventional dates
    :param target_col: the target column where the fixed date would be stored
    :return: list of DataFrames with the fixed dates
    """
    columns_used = json_data[json_section]
    old_date = json_data[json_additional_section]
    fixed_dates_in_dfs = []
    for df in cleaned_dfs_before_fixing_dates:
        for value in columns_used.values():
            if (pd.to_datetime(df[df.columns[value[src_col]]], errors=error_type)).astype(str).str.contains(
                    value[old_year]).all():  # fix excel dates that start with 44
                df[df.columns[value[target_col]]] = pd.TimedeltaIndex(df[df.columns[value[src_col]]],
                                                                      unit=unit_type) + dt.datetime(
                    old_date)  # convert 1970 to date
            else:
                df[df.columns[value[target_col]]] = (
                    pd.to_datetime(df[df.columns[value[src_col]]], errors=error_type)).dt.date
        fixed_dates_in_dfs.append(df)
    return fixed_dates_in_dfs


def convert_to_month_year(clean_df_after_fix_date: List[pd.DataFrame],
                          col_num: int = 1,
                          month_period: str = 'M') -> List[pd.DataFrame]:
    """
    This function converts a full date into month-year date under a particular column
    :param clean_df_after_fix_date: list of DataFrames
    :param col_num: the index value of the column needed where the original date is located
    :param month_period: value of 'M' to represent months
    :return: list of DataFrames, where a particular column is now set to month-year
    """
    convert_date_to_month_year = []
    for df in clean_df_after_fix_date:
        df[df.columns[col_num]] = (pd.to_datetime(df[df.columns[col_num]])).dt.to_period(month_period)
        convert_date_to_month_year.append(df)
    return convert_date_to_month_year


def remove_time(clean_df_after_month_conversion: List[pd.DataFrame],
                json_data,
                json_section: str = 'remove_time',
                error_type: str = 'coerce') -> List[pd.DataFrame]:
    """
    This function removes the time portion from the full date under particular columns
    :param clean_df_after_month_conversion: list of DataFrames
    :param json_data: contains the constants values needed in a form of a dictionary
    :param json_section: the actual section needed in the Json file
    :param error_type: set to 'coerce' to avoid errors when using to_datetime
    :return: list of DataFrames, where the time portion was removed from the full date under particular columns
    """
    cols_to_remove_time = json_data[json_section]
    df_without_time = []
    for df in clean_df_after_month_conversion:
        for i in cols_to_remove_time:
            df[df.columns[i]] = (pd.to_datetime(df[df.columns[i]],
                                                infer_datetime_format=True,
                                                errors=error_type)).dt.date
        df_without_time.append(df)
    return df_without_time


def add_safe_folders(path_of_files: List[str]) -> List[str]:
    """
    This function extract the folder names (safe folders names) from the paths
    :param path_of_files: list of string with the paths where the folders are located
    :return: list of the names of the folders (safes)
    """
    extract_safe_folders = []
    for i in path_of_files:
        path_without_extension = Path(i).parents[0]
        safe_folders = Path(path_without_extension).stem
        extract_safe_folders.append(safe_folders)
    return extract_safe_folders


def fill_safe_names(clean_dfs_with_fixed_dates: List[pd.DataFrame],
                    safe_names: List[str],
                    col_name: str = 'safe_name') -> List[pd.DataFrame]:
    """
    This function fills the safe folders values across the safe_name column
    :param clean_dfs_with_fixed_dates: list of DataFrames
    :param safe_names: list of the safe names
    :param col_name: the column where the values of the safe name would be inserted
    :return: list of DataFrames with the filled safes names under the safe_name column
    """
    clean_dfs_with_safe_names = []
    for df, i in zip(clean_dfs_with_fixed_dates, safe_names):
        df[col_name] = i
        clean_dfs_with_safe_names.append(df)
    return clean_dfs_with_safe_names


def add_file_names(path_of_files: List[str]) -> List[str]:
    """
    This function extracts the file names without extensions (form the paths)
    :param path_of_files: list of the paths of the files
    :return: list of file names
    """
    extract_file_names = []
    for i in path_of_files:
        file_names = Path(i).stem
        extract_file_names.append(file_names)
    return extract_file_names


def fill_file_names(clean_dfs_with_fixed_dates_and_safe_names: List[pd.DataFrame],
                    file_names: List[str],
                    col_name: str = 'file_name') -> List[pd.DataFrame]:
    """
    This function fills the file names values across the file_name column
    :param clean_dfs_with_fixed_dates_and_safe_names: list of DataFrames
    :param file_names: list of the file names
    :param col_name: the column name where the file names would be stored
    :return: list of DataFrames populated with the file names under the file_name column
    """
    clean_dfs_with_safe_and_file_names = []
    for df, i in zip(clean_dfs_with_fixed_dates_and_safe_names, file_names):
        df[col_name] = i
        clean_dfs_with_safe_and_file_names.append(df)
    return clean_dfs_with_safe_and_file_names


def replace_values_yield_at_the_end(clean_dfs_safe_and_file_names: List[pd.DataFrame],
                                    json_data,
                                    json_section_first = 'replace_values_yield_at_the_end_first',
                                    json_section_second = 'replace_values_yield_at_the_end_second',
                                    col_needed: int = 13) -> List[pd.DataFrame]:
    """
    This function replaces the values under the yield_at_the_end column from string to numeric:
    :param clean_dfs_safe_and_file_names: list of DataFrames
    :param json_data: contains the constants values needed in a form of a dictionary
    :param json_section_first: the first section needed in the Json file
    :param json_section_second: the second section needed in the Json file
    :param col_needed: the index of the column where the values need to be replaced
    :return: list of DataFrames where the values under the yield_at_the_end column were replaced from string to numeric
    """
    values_to_replace = json_data[json_section_first]
    new_values_after_replace = json_data[json_section_second]
    replaced_values = []
    for df in clean_dfs_safe_and_file_names:
        df[df.columns[col_needed]] = df[df.columns[col_needed]].replace(values_to_replace, new_values_after_replace)
        replaced_values.append(df)
    return replaced_values


def concat_dataframes(cleaned_dfs_after_fixing_all: List[pd.DataFrame]) -> pd.DataFrame:
    """
    This function concat all DataFrames into a single DataFrame
    :param cleaned_dfs_after_fixing_all: list of DataFrames
    :return: single DataFrame
    """
    df = []
    if cleaned_dfs_after_fixing_all:  # Need this 'if' in case there are no files in the folders
        df = pd.concat(cleaned_dfs_after_fixing_all)
    return df


def rounding(cleaned_dfs_after_fixing_all: List[pd.DataFrame],
             final_concat_df: pd.DataFrame,
             round_to: int = 9):
    """
    This function rounds all the numeric columns (datatype of double) to 9 decimal points
    to suppress the scientific notation
    :param cleaned_dfs_after_fixing_all: list of DataFrames
    :param final_concat_df: single DataFrame
    :param round_to: number of decimal places to round the numeric columns
    :return: single DataFrame, where the numeric columns are limited to 9 decimal places
    """
    df = []
    if cleaned_dfs_after_fixing_all:
        df = final_concat_df.round(round_to)
    return df


def create_csv_for_datalake(final_cleaned_dfs: List[pd.DataFrame],
                            concat_and_rounded: pd.DataFrame,
                            datalake_drop_path: str,
                            file_type: str = 'TXT',
                            sep_type: str = ';',
                            encoding_type: str = 'utf-8-sig'
                            ):
    """
    This function export the file to be loaded into the datalake
    :param final_cleaned_dfs: list of DataFrames - used in an 'if' to avoid an error
    in case there are no files in the folders
    :param concat_and_rounded: single concat DataFrame to be exported as a file
    :param datalake_drop_path: the DL path from the config file where the file will be exported
    :param file_type: CSV or TXT
    :param sep_type: the separator type
    :param encoding_type: 'utf-8-sig' to address hebrew
    :return: An exported file with the entire data to be loaded into the datalake
    """
    create_csv = ''
    if final_cleaned_dfs:  # Need this 'if' in case there are no files in the folders
        create_csv = concat_and_rounded.to_csv(f'{datalake_drop_path}.{file_type}',
                                               index=False,
                                               header=None,
                                               sep=sep_type,
                                               encoding=encoding_type)
    return create_csv


def clear_files(list_of_paths_of_files: List[str],
                file_names: List[str],
                safe_names: List[str],
                cleared_folder: str,
                file_type: str = 'xlsm'):
    """
    This function clears/moves all the files from the individual safes folders
    and save them in a cleared folder while renaming the files to include the safes names
    :param list_of_paths_of_files: list of paths of all the needed files from the safes
    :param file_names: list of the actual file names
    :param safe_names: list of the actual safes names
    :param cleared_folder: the path to the folder where the files would be cleared/moved
    :param file_type: the file type which is xlsm for Excel macro files
    :return: move all the files from the individual safes folders
    and save them in a cleared folder while renaming the files to include the safes names
    """
    for x, y, z in zip(list_of_paths_of_files, file_names, safe_names):
        shutil.move(x, f'{cleared_folder}{y}_{z}.{file_type}')


def select_first_columns(final_cleaned_dfs: List[pd.DataFrame],
                         final_concat_df: pd.DataFrame,
                         first_cols_needed: int = 9) -> pd.DataFrame:
    """
    This function keeps the first 9 columns in the DataFrame
    :param final_cleaned_dfs: list of DataFrames - used in an 'if' to avoid an error
    :param final_concat_df: single concat DataFrame
    :param first_cols_needed: the number of the first columns to keep - here it is 9
    :return: DataFrame with 9 columns
    """
    first_columns = ''
    if final_cleaned_dfs:
        first_columns = final_concat_df.iloc[:, :first_cols_needed]
    return first_columns


def drop_duplicates(final_cleaned_dfs: List[pd.DataFrame],
                    log_df: pd.DataFrame) -> pd.DataFrame:
    """
    This function drops any duplicates rows in the DataFrame
    :param final_cleaned_dfs: list of DataFrames - used in an 'if' to avoid an error
    :param log_df: DataFrame without the duplicate rows
    :return: DataFrame without the duplicate rows
    """
    if final_cleaned_dfs:
        log_df.drop_duplicates(inplace=True)
    return log_df


def export_log_report(final_cleaned_dfs: List[pd.DataFrame],
                      log_distinct_df: pd.DataFrame,
                      export_log_path: str,
                      file_type: str = 'TXT',
                      sep_type: str = ';',
                      encoding_type: str = 'utf-8-sig'
                      ):
    """
    This function exports the log report to be loaded into the DL
    :param final_cleaned_dfs: list of DataFrames - used in an 'if' to avoid an error
    :param log_distinct_df: DataFrame without the duplicates
    :param export_log_path: The path where the log would be exported - defined in config
    :param file_type: CSV or TXT
    :param sep_type: the separator type
    :param encoding_type: 'utf-8-sig' to address hebrew
    :return: An exported file with the log of files to be loaded into the datalake
    """
    create_report = ''
    if final_cleaned_dfs:
        create_report = log_distinct_df.to_csv(f'{export_log_path}.{file_type}',
                                               index=False,
                                               header=None,
                                               sep=sep_type,
                                               encoding=encoding_type)
    return create_report


def export_log_incorrect_version(df_of_excluded_paths: pd.DataFrame,
                                 diff_version_inspectors: List[str],
                                 diff_version_dates: List[str],
                                 export_log_path_incorrect_version: str,
                                 file_type: str = 'TXT',
                                 sep_type: str = ';',
                                 encoding_type: str = 'utf-8-sig',
                                 col_name_time: str = 'process_time',
                                 col_name_inspector: str = 'inspector_name',
                                 col_name_report_date: str = 'current_month_date'
                                 ):
    """
    This function exports the log report of the files that had incorrect file versions
    :param df_of_excluded_paths: DataFrame of the paths of files that were excluded due to incorrect file versions
    :param diff_version_inspectors: list of inspectors names of the files that were not loaded due to incorrect version
    :param diff_version_dates: list of report dates of the files that were not loaded due to incorrect version
    :param export_log_path_incorrect_version: The path where the log would be exported - defined in config
    :param file_type: CSV or TXT
    :param sep_type: the separator type
    :param encoding_type: 'utf-8-sig' to address hebrew
    :param col_name_time: new column name that includes a timestamp
    :param col_name_inspector: new column name that includes the inspector name
    :param col_name_report_date: new column that includes the report date
    :return: An exported file with the log of files that had incorrect file versions
    """
    create_report = ''
    df_of_excluded_paths[col_name_time] = dt.datetime.now()
    df_inspectors = pd.DataFrame(diff_version_inspectors, columns=[col_name_inspector])
    df_report_date = pd.DataFrame(diff_version_dates, columns=[col_name_report_date])
    df_insp_date = pd.merge(df_inspectors, df_report_date, left_index=True, right_index=True)
    df = pd.merge(df_of_excluded_paths, df_insp_date, left_index=True, right_index=True)
    if not df.empty:
        create_report = df.to_csv(f'{export_log_path_incorrect_version}.{file_type}',
                                  index=False,
                                  header=None,
                                  sep=sep_type,
                                  encoding=encoding_type)
    return create_report


def files_age(path_to_cleared_folder: str,
              date_format: str = "%a %b %d %H:%M:%S %Y") -> Dict[str, int]:
    """
    This function creates a dictionary with the file paths (the keys) in the cleared folder and the age
    of the files (the values) in days (age = today vs the original creation date)
    :param path_to_cleared_folder: the clear_after path in the config file where the files will be
    saved after they get processed
    :param date_format: the date format
    :return: dictionary with the file paths (the keys) and the age of the files (the values)
    """
    files_paths = Path(path_to_cleared_folder).glob('**/*')
    files_times = {}
    for i in files_paths:
        files_paths = Path(i)
        creation_time = files_paths.stat().st_mtime
        convert_time = dt.datetime.strptime(time.ctime(creation_time), date_format)
        time_delta = (dt.datetime.now() - convert_time).days
        files_times[str(files_paths)] = time_delta
    return files_times


def files_paths_over_x_days(files_times: Dict[str, int],
                            x_days: int = 90) -> List[str]:
    """
    This function collects only the paths where the files age is above 31 (1 month).
    Those files will be deleted from the cleared_folder
    :param files_times: dictionary with the file paths (the keys) and the age of the files (the values)
    :param x_days: the number of days over which the files will be deleted
    :return: list with only the paths of the files to be deleted
    """
    paths_to_delete = []
    for key, value in files_times.items():
        if value > x_days:
            paths_to_delete.append(key)
    return paths_to_delete


def files_to_delete_after_x_days(paths_of_files_to_delete: List[str]):
    """
    This function deletes the old files from the cleared folder where the age of the files is
    above x days
    :param paths_of_files_to_delete: list of the paths of the files to be deleted
    """
    for i in paths_of_files_to_delete:
        Path(i).unlink()


def permissions(json_data,
                json_section: str = 'permissions',
                cmd_to_lake: str = 'cd /home/talenduser/CSV/'):
    """
    This function provides permissions to the files to be loaded into the DL by running
    the linux command chmod 777 for each file
    :param json_data: contains the constants values needed in a form of a dictionary
    :param json_section: the section needed in the Json file
    :param cmd_to_lake: point to the needed path using 'cd'
    :return: provide permissions to the files to be loaded into the DL
    """
    linux_cmd = json_data[json_section]
    Popen(cmd_to_lake, shell=True).wait()
    for i in linux_cmd:
        x = Popen(i, shell=True).wait()


def export_incorrect_version_metadata(json_data,
                                      export_log_path_incorrect_version: str,
                                      json_section: str = 'incorrect_version_metadata',
                                      file_type: str = 'METADATA',
                                      type_write: str = 'w'):
    """
    This function creates a meatdata file for the incorrect version file
    :param json_data: the content of the metadata file is located in the JSON file
    :param export_log_path_incorrect_version: the path where the REALESTATE_INCORRECT_VERSION.METADATA
    file will be created - as taken from the config ini file
    :param json_section: the section where the metadata content is located
    :param file_type: the file type is METADATA
    :param type_write: w for write
    :return: meatdata file for the incorrect version file
    """
    metadata = json_data[json_section]
    metadata = json.dumps(metadata, separators=(',', ':'))  # add double quotes and remove whitespaces
    meta_file = open(f'{export_log_path_incorrect_version}.{file_type}', type_write)
    meta_file.write(str(metadata))
    meta_file.close()
    return meta_file


def export_inspectors_log_metadata(json_data,
                                   export_path_inspectors_log: str,
                                   json_section: str = 'inspectors_log',
                                   file_type: str = 'METADATA',
                                   type_write: str = 'w'):
    """
    This function creates a meatdata file for the inspectors log file
    :param json_data: the content of the metadata file is located in the JSON file
    :param export_path_inspectors_log: the path where the REALESTATE_INSPECTORS_LOG.METADATA
    file will be created - as taken from the config ini file
    :param json_section: the section where the metadata content is located
    :param file_type: the file type is METADATA
    :param type_write: w for write
    :return: meatdata file for the inspectors log file
    """
    metadata = json_data[json_section]
    metadata = json.dumps(metadata, separators=(',', ':'))  # add double quotes and remove whitespaces
    meta_file = open(f'{export_path_inspectors_log}.{file_type}', type_write)
    meta_file.write(str(metadata))
    meta_file.close()
    return meta_file


def export_inspectors_metadata(json_data,
                               export_path_inspectors: str,
                               json_section: str = 'inspectors',
                               file_type: str = 'METADATA',
                               type_write: str = 'w'):
    """
    This function creates a meatdata file for the inspectors file
    :param json_data: the content of the metadata file is located in the JSON file
    :param export_path_inspectors: the path where the REALESTATE_INSPECTORS.METADATA
    file will be created - as taken from the config ini file
    :param json_section: the section where the metadata content is located
    :param file_type: the file type is METADATA
    :param type_write: w for write
    :return: meatdata file for the inspectors file
    """
    metadata = json_data[json_section]
    metadata = json.dumps(metadata, separators=(',', ':'))  # add double quotes and remove whitespaces
    meta_file = open(f'{export_path_inspectors}.{file_type}', type_write)
    meta_file.write(str(metadata))
    meta_file.close()
    return meta_file


def copy_files_between_servers(json_data,
                               json_section: str = 'copy_files_servers'):
    """
    This function runs a linux command to copy the files in the prod edge01 server to prod edge02 server
    for loading into the dl
    :param json_data: contains the linux command needed
    :param json_section: the section needed in the Json file
    :return: copy the files in the prod edge01 server to prod edge02 server
    """
    linux_cmd = json_data[json_section]
    Popen(linux_cmd, shell=True).wait()


def main(path_to_safe: int = 0,
         datalake_drop_path: int = 1,
         cleared_folder: int = 2,
         export_log_path: int = 3,
         export_incorrect_versions: int = 4
         ) -> str:
    ini_paths = get_config_values_ini()
    json_data = get_json()

    # Get a list that contains the paths of all the sub-folders of the safes
    final_list_before = list_sub_folders(ini_paths[path_to_safe])
    # Get a list of lists that contains the full paths of the Excel xlsm files that are stored in the safes sub folders
    final_full_path_before = full_path_files(final_list_before)
    # Flatten list of list into a list that contains the full paths of the excel files
    final_full_path_flatten_before = flatten_list_of_lists(final_full_path_before)
    # List that contains full paths of the Excel files that match a specific regex pattern: p_xxxxx_ddmmyy
    files_filter_regex = filter_files_regex(final_full_path_flatten_before)
    # List of DataFrames based on the information that was originally stored in the Excel files
    read_excel_files_as_dfs = list_of_dfs(files_filter_regex)
    # Dictionary where the keys are the files paths and the values are the dfs
    dic_path_dfs = dict_paths_and_dfs(files_filter_regex, read_excel_files_as_dfs)
    # Keep a List of DataFrames for a specific version located in cell G1 in the Excel files
    version_cnt_df = version_control_dfs(read_excel_files_as_dfs)
    # List of the paths of all the files needed based on the latest version defined
    all_needed_files = version_control_files(dic_path_dfs)
    # This function creates list of the paths of all the files that were excluded (not loaded to the DL)
    all_files_excluded_due_to_diff_version = diff_version_files_checks(dic_path_dfs)
    # List of the inspectors names for all the files that were excluded due to incorrect version
    diff_version_inspectors_names = diff_version_inspectors(dic_path_dfs)
    # List of the report dates for all the files not loaded because of incorrect version
    diff_version_date = diff_version_report_date(dic_path_dfs)
    # List of dates after converting them from datetime.datetime obj to string
    diff_version_date_convert_datetime = convert_datetime_obj_to_date_str(diff_version_date)
    # Convert list to df, to be exported as a new log of the files that were excluded due to bad version
    convert_list_to_dataframe = convert_list_to_df(all_files_excluded_due_to_diff_version)
    # List of DFs, where the non needed values in the Excel files (like the button and instructions) replaced with nulls
    cleaned_excel = clean_excel_files(version_cnt_df, json_data)
    # List of DataFrames that are now transposed to create a tabular form
    transposed_dfs = transpose_dfs(cleaned_excel)
    # This function fills empty fields with the value located in row with index 2, across the first 40 columns
    fill_the_first_columns = fill_first_columns(transposed_dfs)
    # Fills the total columns with the same values (the data is coming from L45 to L60 non-sequential)
    fill_the_totals_columns = fill_totals_columns(fill_the_first_columns, json_data)
    # Drop the columns that are not needed from the DataFrames (usually blank columns)
    drop_not_needed_columns = drop_columns(fill_the_totals_columns, json_data)
    # Add new empty columns to the DataFrames at the beginning of the DataFrames
    add_columns_at_start = add_new_empty_columns(drop_not_needed_columns, json_data)
    # Add a timestamp under the new load_time column
    add_loadtime = add_load_time(add_columns_at_start)
    # Remove the top 3 rows which are not needed (those rows contains the headings in hebrew)
    removing_top_rows = remove_top_rows(add_loadtime)
    # Replace cases where someone placed date with 8 characters instead of integers
    replacing_dates = replace_dates_with_empty(removing_top_rows)
    # Replace all instances where quotes (or ;) are used into empty values. Needed for the DL
    clean_all_dfs = clean_dataframes(replacing_dates, json_data)
    # Fix the dates in particular columns, where the dates were inserted as numbers (starting with 44)
    cleaned_dfs_with_dates = fix_dates(clean_all_dfs, json_data)
    # Convert a full date into month-year date under a particular column
    clean_df_with_month_date = convert_to_month_year(cleaned_dfs_with_dates)
    # Remove the time portion from the full date under particular columns
    clean_df_time_removed = remove_time(clean_df_with_month_date, json_data)
    # Extract the folder names (safe folders names) from the paths
    extract_all_safe_folders = add_safe_folders(all_needed_files)
    # Fill the safe folders values across the safe_name column
    cleaned_dfs_with_safe_names = fill_safe_names(clean_df_time_removed, extract_all_safe_folders)
    # This function extracts the file names without extensions (form the paths)
    extract_all_file_names = add_file_names(all_needed_files)
    # Fill the file names values across the file_name column
    cleaned_dfs_with_safe_and_file_names = fill_file_names(cleaned_dfs_with_safe_names, extract_all_file_names)
    # Replace the values under the yield_at_the_end column from string to numeric
    cleaned_dfs_with_replace = replace_values_yield_at_the_end(cleaned_dfs_with_safe_and_file_names, json_data)
    # Concat all DataFrames into a single DataFrame
    concat_all_dfs = concat_dataframes(cleaned_dfs_with_replace)
    # Rounds all the numeric columns (datatype of double) to 9 decimal points to suppress scientific notation
    concat_and_rounded = rounding(cleaned_dfs_with_replace, concat_all_dfs)
    # Export the file to be loaded into the DL
    create_csv_for_datalake(clean_all_dfs, concat_and_rounded, ini_paths[datalake_drop_path])
    # clear/move all the files from the individual safes folders  while renaming the files to include the safes names
    clear_files(all_needed_files, extract_all_file_names, extract_all_safe_folders, ini_paths[cleared_folder])
    # Keep the first 9 columns in the DataFrame for the log of the files loaded into the DL
    log_report_select = select_first_columns(clean_all_dfs, concat_all_dfs)
    # Drop any duplicates rows in the DataFrame
    log_without_duplicates = drop_duplicates(clean_all_dfs, log_report_select)
    # Export the log report to be loaded into the DL - the log includes the dates, files and safes
    export_log_report(clean_all_dfs, log_without_duplicates, ini_paths[export_log_path])
    # Export the log report of the files that had incorrect file versions
    export_log_incorrect_version(convert_list_to_dataframe,
                                 diff_version_inspectors_names,
                                 diff_version_date_convert_datetime,
                                 ini_paths[export_incorrect_versions])
    # Dictionary with the file paths (the keys) and the age of the files (the values)
    age_of_the_files = files_age(ini_paths[cleared_folder])
    # List with only the paths of the files to be deleted from the cleared_folder
    path_of_files_over_x_days = files_paths_over_x_days(age_of_the_files)
    # Delete the old files from the cleared folder
    # files_to_delete_after_x_days(path_of_files_over_x_days)
    # Provide permissions to the files to be loaded into the DL by running - the linux command chmod 777
    permissions(json_data)
    # creates a meatdata file for the incorrect version file
    export_incorrect_version_metadata(json_data, ini_paths[export_incorrect_versions])
    # creates a meatdata file for the inspectors log file
    export_inspectors_log_metadata(json_data, ini_paths[export_log_path])
    # creates a meatdata file for the inspectors file
    export_inspectors_metadata(json_data, ini_paths[datalake_drop_path])
    # Runs a linux command to copy the files in the prod edge01 server to prod edge02 server
    copy_files_between_servers(json_data)
    final_process = 'Process Finished'
    return final_process


if __name__ == "__main__":
    main()