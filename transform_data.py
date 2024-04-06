import json
import hashlib

import pandas as pd

# file_path = '2024-04-04 23_08_25_health_metrics.json'
file_path = 'datasets/2024-03-24 20_54_30_health_metrics.json'

with open(file_path, 'r') as file:
    data = json.load(file)

def _parse_elevation_change(df):
  if ('elevationUp' in df.columns) & ('elevationDown' in df.columns):
    df[['elevationUp', 'elevationDown']] = df[['elevationUp', 'elevationDown']].fillna(0)

    df['elevation_change'] = df['elevationUp'] - df['elevationDown']
    return df
  else:
    return df
  

# column_mappings = {
#     'name': 'workout_name',
#     'start': 'started_at',
#     'end': 'completed_at',
#     'location': 'location',
#     'distance': 'distance_mi',
#     'duration': 'duration_sec',
#     'temperature': 'temperature_deg_f',
#     'elevation_change': 'elevation_change',
#     'intensity': 'intensity_kcal_per_hr_kg',
# }

# df = df.rename(columns=column_mappings)[column_mappings.values()]

def _generate_surrogate_key(key_string):
    # Concatenate the values of the columns into a single string
    # key_string = str(row['workout_name']) + str(row['started_at'])
    
    # Convert the concatenated string to bytes
    key_bytes = key_string.encode('utf-8')
    
    # Create a hash object
    hash_object = hashlib.md5(key_bytes)
    
    # Get the hexadecimal digest
    hex_digest = hash_object.hexdigest()
    
    # Return the first 8 characters of the digest as the surrogate key
    return hex_digest
  

rows = []
rows_granular = []
workout_id_list = []
workout_name_list = []
timestamp_list = []
category_list = []
key_list = []
value_list = []

for workout in data['data']['workouts']:
    row = {}
    
    # workout_name = workout.get('name')
    # workout_start_dttm = workout.get('start')
    key_string = ''.join((str(workout.get('name')), str(workout.get('start'))))
    workout_id = _generate_surrogate_key(key_string)
    row['workout_id'] = workout_id
    for key, value in workout.items():
        if type(value) == str:
            row[key] = value
            
        elif type(value) == dict:
            qty = value['qty']
            units = value['units']
            qty_col = ''.join((key, '_qty'))
            units_col = ''.join((key, '_units'))
            row[qty_col] = qty
            row[units_col] = units
            
        elif type(value) == list:
            row_granular = {}
            
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
    rows.append(row)

df = pd.DataFrame(rows)
            
df_granular = pd.DataFrame({
    "workout_id" : workout_id_list,
    "workout_name" : workout_name_list,
    "timestamp": timestamp_list,
    "category": category_list,
    "key": key_list,
    "value": value_list
})
df_granular = df_granular[~df_granular['key'].isin(['date', 'timestamp'])].reset_index(drop=True)

def _capture_duplicates_for_monitoring(df):
    # Capture duplicates to monitor - how common are duplicates being captured
    return df
df_granular_pivot = pd.pivot((df_granular
                              .drop_duplicates(subset=['workout_id', 'workout_name', 'timestamp', 'category', 'key'], keep='last')
                              .reset_index(drop=True)),
                             values='value', 
                             index=['workout_id', 'workout_name', 'timestamp', 'category'],
                             columns=['key'])

def _clean_column_names(df):
    new_column_list = []
    for col in df.columns:
        strs = [f'_{char.lower()}' if (char.upper() == char) & (char != '_') else char for char in col]
        new_col = ''.join(strs)
        new_col = new_col[1:] if new_col[0] == '_' else new_col 
        new_column_list.append(new_col)
    return new_column_list

df.columns = _clean_column_names(df)
df_granular_pivot.columns = _clean_column_names(df_granular_pivot)
print(df.columns)
print(df_granular_pivot.columns)


# Display DataFrame
df.to_csv('datasets/workouts_summary.txt', sep=',')
df_granular.to_csv('datasets/test.txt', sep=',')
df_granular_pivot.to_csv('datasets/test_pivot.txt', sep=',')

with open('column_mappings.json', 'r') as file:
    column_mappings = json.load(file)
print(column_mappings)
print(df.columns)
# df = df.rename(columns=column_mappings["column_mappings"])[column_mappings["column_mappings"].values()]
# print(df.columns)