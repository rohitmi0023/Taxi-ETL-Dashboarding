import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import logging

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)


def load_dataframe_to_bigquery(df, project_id, dataset_id, table_id, 
                              credentials_path=None, if_exists='replace'):
    """
    Load a pandas DataFrame to Google BigQuery
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to upload
    project_id : str
        Your Google Cloud Project ID
    dataset_id : str
        BigQuery dataset ID
    table_id : str
        BigQuery table ID
    credentials_path : str, optional
        Path to service account JSON file (if not using default credentials)
    if_exists : str, default 'replace'
        What to do if table exists: 'replace', 'append', or 'fail'
    """
    
    try:
        # Initialize BigQuery client
        if credentials_path:
            # Use service account credentials
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            client = bigquery.Client(project=project_id, credentials=credentials)
        else:
            # Use default credentials (ADC - Application Default Credentials)
            client = bigquery.Client(project=project_id)
        
        # Create dataset reference
        dataset_ref = client.dataset(dataset_id)
        
        # Check if dataset exists, create if it doesn't
        try:
            client.get_dataset(dataset_ref)
            logger.debug(f"Dataset {dataset_id} already exists")
        except:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set your preferred location
            dataset = client.create_dataset(dataset)
            logger.debug(f"Created dataset {dataset_id}")
        
        # Create table reference
        table_ref = dataset_ref.table(table_id)
        
        # Configure load job
        job_config = bigquery.LoadJobConfig()
        
        # Set write disposition based on if_exists parameter
        if if_exists == 'replace':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        elif if_exists == 'append':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif if_exists == 'fail':
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        
        # Auto-detect schema from DataFrame
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.PARQUET
        
        # Load DataFrame to BigQuery
        logger.debug(f"Loading {len(df)} rows to {project_id}.{dataset_id}.{table_id}...")
        
        job = client.load_table_from_dataframe(
            df, 
            table_ref, 
            job_config=job_config
        )
        
        # Wait for the job to complete
        job.result()
        
        # Get table info
        table = client.get_table(table_ref)
        logger.info(f"Successfully loaded {table.num_rows} rows and {len(table.schema)} columns")
        logger.info(f"Table: {table.project}.{table.dataset_id}.{table.table_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error loading DataFrame to BigQuery: {str(e)}")
        return False

def load_dataframe_to_bigquery_streaming(df, project_id, dataset_id, table_id,
                                        credentials_path=None, chunk_size=10000):
    """
    Load large DataFrame to BigQuery using streaming inserts (for real-time data)
    Note: Streaming inserts have costs and quotas - use load_table_from_dataframe for batch loads
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to upload
    project_id : str
        Your Google Cloud Project ID
    dataset_id : str
        BigQuery dataset ID
    table_id : str
        BigQuery table ID
    credentials_path : str, optional
        Path to service account JSON file
    chunk_size : int, default 10000
        Number of rows to insert per batch
    """
    
    try:
        # Initialize BigQuery client
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            client = bigquery.Client(project=project_id, credentials=credentials)
        else:
            client = bigquery.Client(project=project_id)
        
        # Get table reference
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        
        # Convert DataFrame to list of dictionaries
        rows_to_insert = df.to_dict('records')
        
        # Insert in chunks
        total_rows = len(rows_to_insert)
        inserted_rows = 0
        
        for i in range(0, total_rows, chunk_size):
            chunk = rows_to_insert[i:i + chunk_size]
            errors = client.insert_rows_json(table, chunk)
            
            if errors:
                logger.error(f"Errors inserting rows {i} to {i + len(chunk)}: {errors}")
                return False
            else:
                inserted_rows += len(chunk)
                logger.debug(f"Inserted {inserted_rows}/{total_rows} rows")
        
        logger.info(f"Successfully streamed {inserted_rows} rows to BigQuery")
        return True
        
    except Exception as e:
        logger.error(f"Error streaming DataFrame to BigQuery: {str(e)}")
        return False

# Example usage
if __name__ == "__main__":
    # Sample DataFrame
    sample_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'salary': [50000.0, 60000.0, 70000.0, 55000.0, 65000.0],
        'department': ['Engineering', 'Sales', 'Marketing', 'Engineering', 'Sales'],
        'hire_date': pd.to_datetime(['2023-01-15', '2022-03-20', '2021-07-10', 
                                   '2023-05-01', '2022-11-30'])
    }
    
    df = pd.DataFrame(sample_data)
    logger.info("Sample DataFrame:")
    logger.info(df)
    logger.info("\nDataFrame Info:")
    logger.info(df.info())
    
    # Configuration
    PROJECT_ID = "radiant-ion-464314-b8"
    DATASET_ID = "sample_check"
    TABLE_ID = "random"
    CREDENTIALS_PATH = "radiant-ion-464314-b8-505df8143336.json"  # Optional
    
    # Method 1: Batch load (recommended for most cases)
    print("\n" + "="*50)
    print("Method 1: Batch Load")
    print("="*50)
    
    success = load_dataframe_to_bigquery(
        df=df,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        credentials_path=CREDENTIALS_PATH,  # Remove this line to use default credentials
        if_exists='replace'  # 'replace', 'append', or 'fail'
    )
    
    if success:
        print("✅ DataFrame successfully loaded to BigQuery!")
    else:
        print("❌ Failed to load DataFrame to BigQuery")
    
    # Method 2: Streaming inserts (for real-time data)
    print("\n" + "="*50)
    print("Method 2: Streaming Inserts")
    print("="*50)
    
    # Uncomment below to test streaming inserts
    success_streaming = load_dataframe_to_bigquery_streaming(
        df=df,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID + "_streaming",
        credentials_path=CREDENTIALS_PATH,
        chunk_size=1000
    )

# Additional helper functions

def check_bigquery_table(project_id, dataset_id, table_id, credentials_path=None):
    """Check if table exists and get basic info"""
    try:
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            client = bigquery.Client(project=project_id, credentials=credentials)
        else:
            client = bigquery.Client(project=project_id)
        
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        
        print(f"Table {table.table_id} exists:")
        print(f"  - Rows: {table.num_rows:,}")
        print(f"  - Columns: {len(table.schema)}")
        print(f"  - Size: {table.num_bytes / (1024**2):.2f} MB")
        print(f"  - Created: {table.created}")
        print(f"  - Modified: {table.modified}")
        
        return True
    except Exception as e:
        print(f"Table not found or error: {str(e)}")
        return False

def optimize_dataframe_for_bigquery(df):
    """
    Optimize DataFrame for BigQuery upload
    - Convert object dtypes to appropriate types
    - Handle missing values
    - Optimize memory usage
    """
    df_optimized = df.copy()
    
    for col in df_optimized.columns:
        # Convert object columns that look like numbers
        if df_optimized[col].dtype == 'object':
            # Try to convert to numeric
            try:
                df_optimized[col] = pd.to_numeric(df_optimized[col], errors='ignore')
            except:
                pass
        
        # Convert datetime columns
        if df_optimized[col].dtype == 'object':
            try:
                df_optimized[col] = pd.to_datetime(df_optimized[col], errors='ignore')
            except:
                pass
    
    # Handle missing values (BigQuery doesn't like NaN in integer columns)
    for col in df_optimized.select_dtypes(include=['int64', 'int32']).columns:
        if df_optimized[col].isnull().any():
            df_optimized[col] = df_optimized[col].astype('Int64')  # Nullable integer
    
    print("DataFrame optimization complete:")
    print(df_optimized.dtypes)
    
    return df_optimized