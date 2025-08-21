This project implements an end-to-end ETL (Extract, Transform, Load) pipeline for NYC taxi trip data, dimensional modeling, and automated loading into Google BigQuery for analytics and dashboarding.

## Features

- **Data Extraction:** Reads raw taxi trip data from CSV files.
- **Data Transformation:** Cleans, optimizes, and models data into facts and dimensions (star schema).
- **BigQuery Integration:** Loads processed data into Google BigQuery using batch and streaming methods.
- **Logging:** Detailed logging for monitoring ETL steps and troubleshooting.
- **Extensible:** Modular code for easy adaptation to new data sources or schema changes.

## Project Structure

- main.py: Main ETL pipeline — reads, transforms, and loads data.
- bq_load.py: Utilities for loading pandas DataFrames to BigQuery.
- datasets: Contains raw taxi data CSV files.
- modelling.txt: Documentation of business requirements and data model.
- app.log: Log file for ETL process.

## Data Model

The project uses a star schema with the following tables:

**Dimensions:**
- `dim_vendor`
- `dim_datetime`
- `dim_pickup_location`
- `dim_dropoff_location`
- `dim_ratecode`
- `dim_payment_type`

**Fact Table:**
- `fact_trip`

See modelling.txt for detailed schema and business logic.

## Requirements

- Python 3.8+
- pandas
- google-cloud-bigquery
- google-auth
- Service account with BigQuery permissions

Install dependencies:
```sh
pip install pandas google-cloud-bigquery google-auth
```

## Usage

1. **Configure Google Cloud credentials:**
   - Place your service account JSON in the project root.
   - Update `CREDENTIALS_PATH` in main.py and bq_load.py if needed.

2. **Prepare your data:**
   - Place your taxi data CSV in the datasets directory.

3. **Run the ETL pipeline:**
   ```sh
   python main.py
   ```

4. **Check BigQuery:**
   - Processed tables will appear in your specified dataset.

## Logging

Logs are written to both the console and app.log.

## Customization

- Modify the data model or transformation logic in main.py.
- Adjust BigQuery loading options in bq_load.py.

## License

This project is for educational and demonstration purposes.

---
