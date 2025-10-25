"""
Food Consumption in Indonesia - ETL Pipeline

This script downloads food consumption data from Kaggle, processes Excel files,
and uploads the consolidated CSV to Supabase S3-compatible storage.

Author: Your Name
Date: 2025-10-25
"""

import os
import dotenv
import glob
import csv
import unicodedata
import re
import logging
from datetime import datetime
from typing import List, Optional
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'etl_pipeline_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
dotenv.load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("SUPABASE_ACCESS_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("SUPABASE_ACCESS_KEY")
AWS_REGION = os.getenv("SUPABASE_REGION")
AWS_ENDPOINT_URL = os.getenv("SUPABASE_URL")
AWS_BUCKET_NAME = os.getenv("SUPABASE_BUCKET_NAME")
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd

def validate_environment() -> bool:
    """
    Validate that all required environment variables are set.
    
    Returns:
        bool: True if all required variables are set, False otherwise
    """
    required_vars = [
        'BUCKET_NAME', 'SUPABASE_ACCESS_ID', 'SUPABASE_ACCESS_KEY',
        'SUPABASE_URL', 'KAGGLE_USERNAME', 'KAGGLE_KEY'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    logger.info("✓ All required environment variables are set")
    return True


def upload_file(file_name: str, folder: Optional[str] = None) -> str:
    """
    Upload a file to S3-compatible storage (Supabase).
    
    Args:
        file_name (str): Local path to the file to upload
        folder (str, optional): Target folder in the bucket. Defaults to root.
    
    Returns:
        str: S3 key (path) of the uploaded file
        
    Raises:
        FileNotFoundError: If the local file doesn't exist
        ClientError: If upload fails
        NoCredentialsError: If AWS credentials are invalid
    """
    logger.info(f"Uploading file: {file_name}")
    
    if not os.path.exists(file_name):
        logger.error(f"File not found: {file_name}")
        raise FileNotFoundError(f"File not found: {file_name}")
    
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=AWS_ENDPOINT_URL,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        # Build S3 key
        if folder:
            s3_key = f"{folder.rstrip('/')}/{os.path.basename(file_name)}"
        else:
            s3_key = os.path.basename(file_name)

        # Upload to S3
        s3.upload_file(file_name, BUCKET_NAME, s3_key)
        
        file_size = os.path.getsize(file_name)
        logger.info(f"✓ Successfully uploaded {file_name} ({file_size:,} bytes) to s3://{BUCKET_NAME}/{s3_key}")
        
        return s3_key
        
    except NoCredentialsError:
        logger.error("Invalid AWS credentials")
        raise
    except ClientError as e:
        logger.error(f"Failed to upload file: {e}")
        raise


def download_kaggle_dataset(dataset: str, output_dir: str = 'datasets', unzip: bool = True) -> List[str]:
    """
    Download a dataset from Kaggle.
    
    Args:
        dataset (str): Kaggle dataset identifier (e.g., 'username/dataset-name')
        output_dir (str): Directory to save downloaded files. Defaults to 'datasets'.
        unzip (bool): Whether to unzip downloaded files. Defaults to True.
    
    Returns:
        List[str]: List of file paths in the output directory
        
    Raises:
        Exception: If download fails or authentication fails
    """
    logger.info(f"Downloading Kaggle dataset: {dataset}")
    
    try:
        api = KaggleApi()
        api.authenticate()
        logger.info("✓ Kaggle API authenticated")
        
        os.makedirs(output_dir, exist_ok=True)
        
        api.dataset_download_files(dataset, path=output_dir, unzip=unzip)
        logger.info(f"✓ Dataset downloaded to {output_dir}")
        
        files = get_list_file(output_dir, "*")
        logger.info(f"✓ Found {len(files)} files in {output_dir}")
        
        return files
        
    except Exception as e:
        logger.error(f"Failed to download Kaggle dataset: {e}")
        raise


def get_list_file(dir: str, ext: str = '*') -> List[str]:
    """
    Get list of files in a directory matching the extension.
    
    Args:
        dir (str): Directory path
        ext (str): File extension (without dot) or '*' for all files. Defaults to '*'.
    
    Returns:
        List[str]: List of matching file paths
    """
    pattern = f"*.{ext}" if ext != '*' else '*'
    files = glob.glob(os.path.join(dir, pattern))
    logger.debug(f"Found {len(files)} files with pattern {pattern} in {dir}")
    return files


def remove_invisible_chars(text) -> str:
    """
    Remove invisible and non-printable characters from text.
    
    Args:
        text: Input text (any type, but only strings are processed)
    
    Returns:
        str: Cleaned text with invisible characters removed
    """
    if not isinstance(text, str):
        return text

    text = unicodedata.normalize("NFKC", text)
    text = ''.join(ch for ch in text if ch.isprintable())
    text = re.sub(r'[\u200E\u200F\u202A-\u202E\u2066-\u2069\u200B-\u200F\uFEFF]', '', text)
    text = text.replace('\u00A0', ' ').replace('\u180E', '')

    return text


def pd_read_xlsx(excel_files: List[str]) -> pd.DataFrame:
    """
    Read and combine multiple Excel files into a single DataFrame.
    
    Processing steps:
    1. Read each Excel file
    2. Add SOURCE_FILE column
    3. Combine all DataFrames
    4. Normalize column names (replace spaces, remove parentheses)
    5. Convert string values to uppercase
    6. Add PROCESS_DATE column
    7. Remove invisible characters
    
    Args:
        excel_files (List[str]): List of Excel file paths
    
    Returns:
        pd.DataFrame: Combined and processed DataFrame
        
    Raises:
        ValueError: If no Excel files provided
        Exception: If file reading fails
    """
    if not excel_files:
        logger.error("No Excel files provided")
        raise ValueError("No Excel files provided")
    
    logger.info(f"Reading {len(excel_files)} Excel file(s)")
    
    df_list = []
    for i, file in enumerate(excel_files, 1):
        try:
            logger.info(f"  [{i}/{len(excel_files)}] Reading {os.path.basename(file)}")
            df = pd.read_excel(file)
            df["SOURCE_FILE"] = os.path.basename(file)
            df_list.append(df)
            logger.info(f"    ✓ Loaded {len(df):,} rows")
        except Exception as e:
            logger.error(f"    ✗ Failed to read {file}: {e}")
            raise
    
    logger.info("Combining and processing data...")
    combined_df = pd.concat(df_list, ignore_index=True)
    
    # Normalize column names
    combined_df.columns = (
        combined_df.columns
        .str.replace(' ', '_', regex=False)
        .str.replace('(', '', regex=False)
        .str.replace(')', '', regex=False)
    )
    
    # Convert strings to uppercase
    combined_df = combined_df.map(lambda x: x.upper() if isinstance(x, str) else x)
    
    # Add process date
    combined_df["PROCESS_DATE"] = pd.to_datetime("today").normalize()
    
    # Remove invisible characters
    combined_df = combined_df.map(remove_invisible_chars)
    
    logger.info(f"✓ Combined DataFrame: {len(combined_df):,} rows × {len(combined_df.columns)} columns")
    logger.info(f"  Columns: {', '.join(combined_df.columns.tolist())}")
    
    return combined_df


def pd_to_csv(df: pd.DataFrame, dir: str, filename: str) -> str:
    """
    Save DataFrame to CSV file with all fields quoted.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        dir (str): Output directory
        filename (str): Output filename
    
    Returns:
        str: Full path to the saved CSV file
        
    Raises:
        Exception: If file writing fails
    """
    logger.info(f"Saving DataFrame to CSV: {filename}")
    
    try:
        os.makedirs(dir, exist_ok=True)
        filepath = os.path.join(dir, filename)
        
        df.to_csv(filepath, index=False, quoting=csv.QUOTE_ALL)
        
        file_size = os.path.getsize(filepath)
        logger.info(f"✓ CSV saved: {filepath} ({file_size:,} bytes)")
        
        return filepath
        
    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")
        raise


def main() -> bool:
    """
    Main ETL pipeline execution.
    
    Process flow:
    1. Validate environment variables
    2. Download dataset from Kaggle
    3. Read and process Excel files
    4. Save combined data to CSV
    5. Upload CSV to S3 storage
    
    Returns:
        bool: True if pipeline completes successfully
        
    Raises:
        Exception: If any step fails
    """
    logger.info("="*70)
    logger.info("STARTING ETL PIPELINE - Food Consumption in Indonesia")
    logger.info("="*70)
    
    start_time = datetime.now()
    
    try:
        # Configuration
        today_str = datetime.now().strftime("%Y%m%d")
        dataset_dir = 'datasets'
        kaggle_dataset = 'saurabhshahane/food-consumption-in-indonesia'
        output_dir = 'outputs'
        output_filename = f'FoodConsumptionInIndonesia_{today_str}.csv'
        
        logger.info(f"Configuration:")
        logger.info(f"  Dataset: {kaggle_dataset}")
        logger.info(f"  Output: {output_filename}")
        logger.info(f"  Bucket: {BUCKET_NAME}")
        
        # Step 1: Validate environment
        logger.info("\n[STEP 1/5] Validating environment variables...")
        if not validate_environment():
            raise EnvironmentError("Environment validation failed")
        
        # Step 2: Download dataset
        logger.info("\n[STEP 2/5] Downloading dataset from Kaggle...")
        excel_files = download_kaggle_dataset(kaggle_dataset, dataset_dir)
        
        # Step 3: Process Excel files
        logger.info("\n[STEP 3/5] Processing Excel files...")
        df = pd_read_xlsx(excel_files)
        
        # Step 4: Save to CSV
        logger.info("\n[STEP 4/5] Saving to CSV...")
        filepath = pd_to_csv(df, output_dir, output_filename)
        
        # Step 5: Upload to S3
        logger.info("\n[STEP 5/5] Uploading to S3...")
        s3_file = upload_file(filepath, folder=output_dir)
        
        # Success summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "="*70)
        logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        logger.info(f"  Duration: {duration:.2f} seconds")
        logger.info(f"  Records processed: {len(df):,}")
        logger.info(f"  Output file: {filepath}")
        logger.info(f"  S3 location: s3://{BUCKET_NAME}/{s3_file}")
        logger.info("="*70)
        
        return True
        
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.error("\n" + "="*70)
        logger.error("✗ PIPELINE FAILED")
        logger.error("="*70)
        logger.error(f"  Error: {str(e)}")
        logger.error(f"  Duration: {duration:.2f} seconds")
        logger.error("="*70)
        
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Pipeline terminated with error: {e}")
        exit(1)