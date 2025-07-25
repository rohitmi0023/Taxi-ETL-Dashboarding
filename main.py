# %%
# !pip3 install pandas

    # %%
import pandas as pd

df = pd.read_csv('taxi_data.csv')

# development
pd.set_option('display.max_columns', None) # show all columns
pd.set_option('display.max_rows', 100) # show 100 rows
pd.set_option('display.width', None) # auto width

# creating a unique identifier for each row
df['trip_id'] = df.index + 1

# convert tpep_pickup_datetime, tpep_dropoff_datetime from object to datetime in a single line
df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']] = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].apply(pd.to_datetime)

df['tpep_pickup_datetime'].dt.day.drop_duplicates()
# get unique pickup days