import json
import hashlib

import pandas as pd
import numpy as np



def _generate_surrogate_key(key_string):
    
    # Convert the concatenated string to bytes
    key_bytes = key_string.encode('utf-8')
    
    # Create a hash object
    hash_object = hashlib.md5(key_bytes)
    
    # Get the hexadecimal digest
    hex_digest = hash_object.hexdigest()
    
    # Return the first 8 characters of the digest as the surrogate key
    return hex_digest
  
def _extract_nested_data(data):
  rows = []
  workout_id_list = []
  workout_name_list = []
  timestamp_list = []
  category_list = []
  key_list = []
  value_list = []

  for workout in data['data']['workouts']:
      print(workout['name'])
      row = {}
      key_string = ''.join((str(workout.get('name')), str(workout.get('start'))))
      workout_id = workout['id'] if 'id' in workout.keys() else _generate_surrogate_key(key_string)
      row['workout_id'] = workout_id
      for key, value in workout.items():
              
          if (type(value) == dict) & (value != {}):
              qty = value['qty']
              units = value['units']
              qty_col = ''.join((key, '_qty'))
              units_col = ''.join((key, '_units'))
              row[qty_col] = qty
              row[units_col] = units
              
          elif type(value) == list:
              
              # Iterate over the JSON data
              for items in value:
                  for sub_key, sub_value in items.items():
                      if sub_key == "date" or sub_key == "timestamp":
                          timestamp_list.append(sub_value)
                      else:
                          timestamp_list.append(items.get("date", items.get("timestamp", None)))
                      
                      workout_id_list.append(workout_id)
                      workout_name_list.append(workout.get('name'))
                      category_list.append(key)
                      key_list.append(sub_key)
                      value_list.append(sub_value)
          else:
              row[key] = value
      rows.append(row)
      
  workout_details_lists = {
    'workout_id_list' : workout_id_list,
    'workout_name_list' : workout_name_list,
    'timestamp_list' : timestamp_list,
    'category_list' : category_list,
    'key_list' : key_list,
    'value_list' : value_list
  }
  return rows, workout_details_lists


     
def _clean_column_names(df):
    new_column_list = []
    for col in df.columns:
        strs = [f'_{char.lower()}' if (char.upper() == char) & (char != '_') else char for char in col]
        new_col = ''.join(strs)
        new_col = new_col[1:] if new_col[0] == '_' else new_col 
        new_column_list.append(new_col)
    return new_column_list

def _transform_datetime_cols(df):
    df['started_at'] = pd.to_datetime(df['started_at'])
    df['completed_at'] = pd.to_datetime(df['completed_at'])
    df['duration_min'] = df['duration'] / 60
    return df

def _set_column_order(df):
    # Specify the columns that should be at the beginning
    cols_to_set_first = ['workout_id', 'workout_name', 'started_at', 
                        'completed_at', 'duration_min', 'location']
    cols_to_remove = ['id', 'duration', 'elevation_up_qty', 'elevation_up_units',
                      'elevation_down_qty', 'elevation_down_units']
    
    # Create a list of the remaining columns
    remaining_cols = [col for col in df.columns if (col not in cols_to_set_first) & (col not in cols_to_remove)]

    # Combine the lists to form the new column order
    col_order = cols_to_set_first + remaining_cols

    return col_order

def calculate_elevation_change(df):
    # Calculate elevation change based on conditions
    df['elevation_change_qty'] = np.nan
    df['elevation_change_units'] = np.nan
    
    if 'elevation_up_qty' not in df.columns:
        df['elevation_up_qty'] = np.nan
        df['elevation_up_units'] = np.nan
    
    if 'elevation_down_qty' not in df.columns:
        df['elevation_down_qty'] = np.nan
        df['elevation_down_units'] = np.nan
    
    # If both elevation_up_qty and elevation_down_qty are not NA, find the difference
    mask_both_not_na = ~df['elevation_up_qty'].isna() & ~df['elevation_down_qty'].isna()
    df.loc[mask_both_not_na, 'elevation_change_qty'] = df.loc[mask_both_not_na, 'elevation_up_qty'] - df.loc[mask_both_not_na, 'elevation_down_qty']
    df.loc[mask_both_not_na, 'elevation_change_units'] = df.loc[mask_both_not_na, 'elevation_up_units']

    # If elevation_up_qty is not NA and elevation_down_qty is NA, set elevation_change_qty to elevation_up_qty
    mask_up_not_na = ~df['elevation_up_qty'].isna() & df['elevation_down_qty'].isna()
    df.loc[mask_up_not_na, 'elevation_change_qty'] = df.loc[mask_up_not_na, 'elevation_up_qty']
    df.loc[mask_up_not_na, 'elevation_change_units'] = df.loc[mask_up_not_na, 'elevation_up_units']

    # If elevation_down_qty is not NA and elevation_up_qty is NA, set elevation_change_qty to negative elevation_down_qty
    mask_down_not_na = df['elevation_up_qty'].isna() & ~df['elevation_down_qty'].isna()
    df.loc[mask_down_not_na, 'elevation_change_qty'] = -df.loc[mask_down_not_na, 'elevation_down_qty']
    df.loc[mask_down_not_na, 'elevation_change_units'] = df.loc[mask_down_not_na, 'elevation_down_units']

    return df
  
def _create_workouts_pivot_df(data):
    _, extracted_details_lists = _extract_nested_data(data)
    df_granular = pd.DataFrame({
        "workout_id" : extracted_details_lists['workout_id_list'],
        "workout_name" : extracted_details_lists['workout_name_list'],
        "timestamp": extracted_details_lists['timestamp_list'],
        "category": extracted_details_lists['category_list'],
        "key": extracted_details_lists['key_list'],
        "value": extracted_details_lists['value_list']
    })
    df_granular = df_granular[~df_granular['key'].isin(['date', 'timestamp'])].reset_index(drop=True)

    df_granular_pivot = pd.pivot((df_granular
                                  .drop_duplicates(
                                    subset=['workout_id', 'workout_name', 
                                            'timestamp', 'category', 'key'], 
                                    keep='last')
                                  .reset_index(drop=True)),
                                values='value', 
                                index=['workout_id', 'workout_name', 
                                       'timestamp', 'category'],
                                columns=['key'])


    df_granular_pivot.columns = _clean_column_names(df_granular_pivot)
    return df_granular_pivot



def _create_workouts_summary_df(data):
    extracted_row_data, _ = _extract_nested_data(data)
    df = pd.DataFrame(extracted_row_data)
    df.columns = _clean_column_names(df)
    df = df.rename(columns=column_mappings)
    df = _transform_datetime_cols(df)
    df = calculate_elevation_change(df)
    df = df[_set_column_order(df)]
    return df
  

# file_path = '2024-04-04 23_08_25_health_metrics.json'
file_path = 'datasets/2024-03-24 20_54_30_health_metrics.json'
# file_path = 'datasets/2024-06-01 04_25_23_workouts.json'

with open(file_path, 'r') as file:
    data = json.load(file)
    
with open('column_mappings.json', 'r') as file:
    column_mappings = json.load(file)
    
df_summary = _create_workouts_summary_df(data)
df_details = _create_workouts_pivot_df(data)

# Write data to files
df_summary.to_csv('datasets/workouts_summary.txt', sep=',')
df_details.to_csv('datasets/workouts_details.txt', sep=',')