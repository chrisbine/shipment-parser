import requests
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
import logging
from io import StringIO
import os
from typing import Optional


# Main file to download, parse, validate and insert data into DB

# Config
DATABASE_FILE = "energy_data.db"    # sqlite database file

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Database table schema (to validate if all columns are needed)
TABLE_NAME = "operational_capacity"
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    post_date DATE,
    effective_date DATE,
    loc TEXT,
    loc_zn TEXT,
    loc_name TEXT,
    loc_purp_desc TEXT,
    loc_qti REAL,
    flow_ind TEXT,
    dc TEXT,
    opc TEXT,
    tsq REAL,
    oac REAL,
    it TEXT,
    auth_overrun_ind TEXT,
    nom_cap_exceed_ind TEXT,
    all_qty_avail TEXT,
    qty_reason TEXT
);
"""

def setup_database() -> None:
    """Create database table if it doesn't exist yet"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()
        conn.close()
        logger.info("SQLite database table setup completed")
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        raise


def download_data_by_date(date: datetime) -> pd.DataFrame:
    """Download data for a specific date and saving it to file"""

    url = "https://twtransfer.energytransfer.com/ipost/TW/capacity/operationally-available"
    params = {
        'f': 'csv',
        'extension': 'csv',
        'asset': 'TW',
        'gasDay': date.strftime('%m/%d/%Y'),
        'searchType': 'NOM'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() # Check Http errors

        data = StringIO(response.text)
        df = pd.read_csv(
            data, 
            encoding='utf-8',
            on_bad_lines='skip',
            na_values=['NA', 'N/A', 'missing']
            )
        
        df.head()
    except Exception as e:
        logger.error(f"Error downloading data into dataframe: {e}")
        raise

    return df



def validate_data(df: pd.DataFrame) -> bool:
    """Validate the downloaded data"""
    required_columns = [
        'Post Date', 'Effective Date', 'Loc', 'Loc Zn', 'Loc Name',
        'Loc Purp Desc', 'Loc/QTI', 'Flow Ind', 'DC', 'OPC', 'TSQ', 'OAC',
        'IT', 'Auth Overrun Ind', 'Nom Cap Exceed Ind', 'All Qty Avail', 'Qty Reason'
    ]
    
    # Check if all required columns are present
    if not all(col in df.columns for col in required_columns):
        missing = [col for col in required_columns if col not in df.columns]
        logger.error(f"Missing required columns: {missing}")
        return False
    
    # Check for empty data
    if df.empty:
        logger.error("DataFrame is empty")
        return False
    
    # Check for null values in critical columns
    critical_columns = ['Post Date', 'Effective Date', 'Loc', 'Loc Zn', 'Loc/QTI']
    if df[critical_columns].isnull().any().any():
        logger.error("Null values found in critical columns")
        return False
    
    return True


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the data before insertion"""
    # Rename columns to match database schema
    column_mapping = {
        'Post Date': 'post_date',
        'Effective Date': 'effective_date',
        'Loc': 'loc',
        'Loc Zn': 'loc_zn',
        'Loc Name': 'loc_name',
        'Loc Purp Desc': 'loc_purp_desc',
        'Loc/QTI': 'loc_qti',
        'Flow Ind': 'flow_ind',
        'DC': 'dc',
        'OPC': 'opc',
        'TSQ': 'tsq',
        'OAC': 'oac',
        'IT': 'it',
        'Auth Overrun Ind': 'auth_overrun_ind',
        'Nom Cap Exceed Ind': 'nom_cap_exceed_ind',
        'All Qty Avail': 'all_qty_avail',
        'Qty Reason': 'qty_reason'
    }
    df = df.rename(columns=column_mapping)
    
    # Convert date columns for readability 
    df['post_date'] = pd.to_datetime(df['post_date']).dt.date
    df['effective_date'] = pd.to_datetime(df['effective_date']).dt.date
    
    # Convert numeric columns
    numeric_cols = ['loc_qti', 'tsq', 'oac']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def insert_data_to_db(df: pd.DataFrame) -> None:
    """Insert the transformed data into SQLite"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        
        # df into temp_table
        df.to_sql(
            'temp_table',
            conn,
            if_exists='replace',
            index=False
        )
        
        # Inserting only new records
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT OR IGNORE INTO {TABLE_NAME}
            SELECT
                post_date ,
                effective_date ,
                loc ,
                loc_zn ,
                loc_name ,
                loc_purp_desc ,
                loc_qti ,
                flow_ind ,
                dc ,
                opc ,
                tsq ,
                oac ,
                it ,
                auth_overrun_ind ,
                nom_cap_exceed_ind ,
                all_qty_avail ,
                qty_reason 
            FROM temp_table
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_NAME} 
                WHERE post_date = temp_table.post_date
                    AND effective_date = temp_table.effective_date
                    AND loc = temp_table.loc
            )
        """)
        
        # Cleaning up
        cursor.execute("DROP TABLE IF EXISTS temp_table")
        conn.commit()
        conn.close()
        logger.info(f"Inserted {cursor.rowcount} new records into database")

    except Exception as e:
        logger.error(f"Error inserting data into database: {e}")
        if 'conn' in locals():
            conn.close()
        raise


def process_last_n_days(n: int) -> None:
    """Main function to process data for the last n days"""
    setup_database()
    
    for days_ago in range(0, n):  # Last n days
        target_date = datetime.now() - timedelta(days=days_ago)
        logger.info(f"Processing data for {target_date.strftime('%Y-%m-%d')}")
        
        # Step 1: Download data into dataframe
        df = download_data_by_date(target_date)
        
        try:
            # 2 : add effective date and post date
            df['Post Date'] = datetime.now()
            df['Effective Date'] = target_date
            
            if not validate_data(df):
                logger.error(f"Validation failed for {target_date.strftime('%Y-%m-%d')}")
                continue
            
            # Step 3: Transform data
            df = transform_data(df)
            
            # Step 4: Insert into database
            insert_data_to_db(df)
            
        except Exception as e:
            logger.error(f"Error processing data for {target_date.strftime('%Y-%m-%d')}: {e}")
            continue


# main process call
if __name__ == "__main__":
    process_last_n_days(3)

    # TODO: Improvements 
    # Validate needed columns and remove unused ones
    # Add data validation to filter bad data / check data types
    # Add option to overwrite/keep DB values if importing the same file
    # Optimize performance / test file types

