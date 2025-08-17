# %%
import pandas as pd
import logging
from pathlib import Path
from typing import List, Dict


file_path = Path('datasets/taxi_data.csv')

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)

def read_csv(file_path: Path) -> pd.DataFrame:
    logger.debug(f"Executing function {read_csv.__name__}....")
    try:
        df = pd.read_csv(file_path)
        logger.info(f'CSV file read successfully of rows {df.shape[0]} and columns {df.shape[1]}')
        return df
    except FileNotFoundError:
        logger.error(f"File {file_path} not found.")
    except Exception as e:
        logger.error(f"An error occured: {e}")


def datetime_conversion(df: pd.DataFrame, columns:List[str]) -> pd.DataFrame:
    logger.debug(f"Executing function {datetime_conversion.__name__}...")
    for col in columns:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            logger.info(f"Column {col} converted to datetime")
        except Exception as e:
            logger.error(f"Error converting column {col} to datetime: {e}")

    logger.debug(f"Completed executing function {datetime_conversion.__name__}")
    return df


def dtype_optimization(df:pd.DataFrame) -> pd.DataFrame:
    logger.debug(f"Executing function {dtype_optimization.__name__}...")
    memory_mb_1 = df.memory_usage(deep=True, index=False).sum() / 1024 / 1024
    logger.debug(f"Initial Memory Usage: {memory_mb_1:.2f} MB")
    columns = df.columns
    for col in columns:
        starting_dtype = df[col].dtype
        if df[col].dtype == 'object':
            if df[col].nunique() / len(df) < 0.05:
                df[col] = df[col].astype('category')
        elif df[col].dtype == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer')
        elif df[col].dtype == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')
        ending_dtype = df[col].dtype
        if starting_dtype != ending_dtype:
            logger.debug(f"Column {col} changed from {starting_dtype} to {ending_dtype}")
    memory_mb_2 = df.memory_usage(deep=True, index=False).sum() / 1024 / 1024
    reduction = ((memory_mb_1 - memory_mb_2)/memory_mb_1) * 100
    logger.info(f"Memory Usage after Optimization: {memory_mb_2:.2f} MB, reduction of {reduction:.2f}%")
    logger.debug(f"Completed executing function {dtype_optimization.__name__}")
    return df


def creating_dimensions(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    logger.debug(f'Executing function {creating_dimensions.__name__}...')

    dimensions:Dict[str, pd.DataFrame] = {}

    try:
        # dim_vendor
        dim_vendor = df[['VendorID']].drop_duplicates().reset_index(drop=True)
        dim_vendor.reset_index(names='dim_vendor_key', inplace=True)
        vendor_dict = {
            1: 'Creative Mobile Technologies, LLC',
            2: 'Curb Mobility, LLC',
            6: 'Myle Technologies Inc',
            7: 'Helix'
        }
        dim_vendor['vendor_name'] = dim_vendor['VendorID'].map(vendor_dict)
        dimensions['dim_vendor'] = dim_vendor
        logger.debug(f'Dimension dim_vendor created with columns {dim_vendor.columns.tolist()} of {dim_vendor.shape[0]} rows and {dim_vendor.shape[1]} columns')

        # dim_datetime
        dim_datetime = pd.DataFrame()
        dim_datetime['full_datetime'] = pd.concat([df['tpep_pickup_datetime'], df['tpep_dropoff_datetime']])
        dim_datetime = dim_datetime.drop_duplicates().reset_index(drop=True)
        dim_datetime.reset_index(names='dim_datetime_key', inplace=True)
        df['hour'] = dim_datetime['full_datetime'].dt.hour
        df['date'] = dim_datetime['full_datetime'].dt.date
        df['day'] = dim_datetime['full_datetime'].dt.day
        df['day_of_week'] = dim_datetime['full_datetime'].dt.day_of_week
        df['day_name'] = dim_datetime['full_datetime'].dt.day_name
        df['year'] = dim_datetime['full_datetime'].dt.year
        df['month_name'] = dim_datetime['full_datetime'].dt.month_name
        df['weekday'] = dim_datetime['full_datetime'].dt.weekday
        df['is_weekend'] =  df['weekday'].isin([5,6])
        df['quarter'] = dim_datetime['full_datetime'].dt.quarter
        df['month'] = dim_datetime['full_datetime'].dt.month
        dimensions['dim_datetime'] = dim_datetime
        logger.debug(f'Dimension dim_datetime created with columns {dim_datetime.columns.tolist()} of {dim_datetime.shape[0]} rows and {dim_datetime.shape[1]} columns')

        # dim_pickup_location
        dim_pickup_location = df[['pickup_latitude', 'pickup_longitude']].drop_duplicates().reset_index(drop=True)
        dim_pickup_location.reset_index(names='dim_pickup_location_key', inplace=True)
        dimensions['dim_pickup_location'] = dim_pickup_location
        logger.debug(f'Dimension dim_pickup_location created with columns {dim_pickup_location.columns.tolist()} of {dim_pickup_location.shape[0]} rows and {dim_pickup_location.shape[1]} columns')

        # dim_dropoff_location
        dim_dropoff_location = df[['dropoff_latitude', 'dropoff_longitude']].drop_duplicates().reset_index(drop=True)
        dim_dropoff_location.reset_index(names='dim_dropoff_location_key', inplace=True)
        dimensions['dim_dropoff_location'] = dim_dropoff_location
        logger.debug(f'Dimension dim_dropoff_location created with columns {dim_dropoff_location.columns.tolist()} of {dim_dropoff_location.shape[0]} rows and {dim_dropoff_location.shape[1]} columns')

        # dim_ratecode
        dim_ratecode = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
        dim_ratecode.reset_index(names='dim_ratecode_key', inplace=True)
        ratecode_dict = {
            1: 'Standard',
            2: 'JFK',
            3: 'Newark',
            4: 'LaGuardia',
            5: 'Negotiated Fare',
            6: 'Group ride',
            99: 'Unknown'
        }
        dim_ratecode['ratecode_description'] = dim_ratecode['RatecodeID'].map(ratecode_dict)
        dimensions['dim_ratecode'] = dim_ratecode
        logger.debug(f'Dimension dim_ratecode created with columns {dim_ratecode.columns.tolist()} of {dim_ratecode.shape[0]} rows and {dim_ratecode.shape[1]} columns')

        # dim_payment_type
        dim_payment_type = df[['payment_type']].drop_duplicates().reset_index(drop=True)
        dim_payment_type.reset_index(names='dim_payment_type_key', inplace=True)
        payment_dict = {
            0: 'Flex Fare trip',
            1: 'Credit Card',
            2: 'Cash',
            3: 'No charge',
            4: 'Dispute',
            5: 'Unknown',
            6: 'Voided_trip'
        }
        dim_payment_type['payment_type_description'] = dim_payment_type['payment_type'].map(payment_dict)
        dimensions['dim_payment_type'] = dim_payment_type
        logger.debug(f'Dimension dim_payment_type created with columns {dim_payment_type.columns.tolist()} of {dim_payment_type.shape[0]} rows and {dim_payment_type.shape[1]} columns')

        logger.debug(f'Completed executing function {creating_dimensions.__name__} with {len(dimensions)} dimensions created')
        return dimensions
    except Exception as e:
        logger.error(f"Error occurred in function {creating_dimensions.__name__}: {e}")
        return {}

def creating_facts(df: pd.DataFrame, dimensions: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    logger.debug(f'Executing function {creating_facts.__name__}.....')

    try:
        # create fact table
        fact_trips = pd.DataFrame()
        fact_trips['trip_id'] = df.index + 1

        # foreign keys to dims
        fact_trips['dim_vendor_key'] = df['VendorID'].map(dimensions['dim_vendor'].set_index('VendorID')['dim_vendor_key'])
        fact_trips['pickup_datetime_key'] = df['tpep_pickup_datetime'].map(dimensions['dim_datetime'].set_index('full_datetime')['dim_datetime_key'])
        fact_trips['dropoff_datetime_key'] = df['tpep_dropoff_datetime'].map(dimensions['dim_datetime'].set_index('full_datetime')['dim_datetime_key'])
        fact_trips['dim_pickup_location_key'] = df[['pickup_latitude', 'pickup_longitude']].apply(tuple, axis=1).map(dimensions['dim_pickup_location'].set_index(['pickup_latitude', 'pickup_longitude'])['dim_pickup_location_key'])
        fact_trips['dim_dropoff_location_key'] = df[['dropoff_latitude', 'dropoff_longitude']].apply(tuple, axis=1).map(dimensions['dim_dropoff_location'].set_index(['dropoff_latitude', 'dropoff_longitude'])['dim_dropoff_location_key'])
        fact_trips['dim_ratecode_key'] = df['RatecodeID'].map(dimensions['dim_ratecode'].set_index('RatecodeID')['dim_ratecode_key'])
        fact_trips['dim_payment_type_key'] = df['payment_type'].map(dimensions['dim_payment_type'].set_index('payment_type')['dim_payment_type_key'])
        
        # measures
        fact_trips['passenger_count'] = df['passenger_count']
        fact_trips['trip_distance'] = df['trip_distance']
        fact_trips['fare_amount'] = df['fare_amount']
        fact_trips['extra'] = df['extra']
        fact_trips['mta_tax'] = df['mta_tax']
        fact_trips['tip_amount'] = df['tip_amount']
        fact_trips['tolls_amount'] = df['tolls_amount']
        fact_trips['improvement_surcharge'] = df['improvement_surcharge']
        fact_trips['total_amount'] = df['total_amount']
        fact_trips['trip_duration_minutes'] = round((df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60, 2)
        fact_trips['store_and_fwd_flag'] = df['store_and_fwd_flag']

        logger.debug(f'Fact table created with {fact_trips.shape[0]} rows and {fact_trips.shape[1]} columns')

        return fact_trips
    
    except Exception as e:
        logger.error(f"Error creating fact table: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    logger.debug("Starting the ETL process")

    df = read_csv(file_path)
 
    if df is not None:
        df = datetime_conversion(df, ['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

        df = dtype_optimization(df)
        dimensions = creating_dimensions(df)

        if dimensions is not None:
            optimized_dimensions = {}

            for dim_name, dim in dimensions.items():
                optimized_dim = dtype_optimization(dim)
                optimized_dimensions[dim_name] = optimized_dim
            logger.debug(f"Optimized dimensions created: {[dim.shape for dim in optimized_dimensions.values()]}")

        facts = creating_facts(df, optimized_dimensions)

        if facts is not None:
            optimized_fact_trips = dtype_optimization(facts)
            logger.debug(f"Optimized facts created with shape {optimized_fact_trips.shape}")




# info
# logger.info(f"DataFrame info: {df.info()}")

# creating a unique identifier for each row and adding it as the first column
# df['trip_id'] = df.index + 1

# display configuration
# pd.set_option('display.max_columns', None) # show all columns
# pd.set_option('display.max_rows', 100) # show 100 rows
# pd.set_option('display.width', None) # auto width


# get unique pickup days
# df['tpep_pickup_datetime'].dt.day.drop_duplicates()
