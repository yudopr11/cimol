# Cimol - Food Consumption ETL Pipeline

A Python ETL pipeline that downloads food consumption data from Kaggle, processes Excel files, and uploads the consolidated data to Supabase S3-compatible storage.

## Overview

This project processes food consumption data from Indonesia by:
1. Downloading datasets from Kaggle
2. Reading and combining multiple Excel files
3. Cleaning and normalizing the data
4. Saving the processed data to CSV
5. Uploading the final dataset to cloud storage

## Prerequisites

- Python 3.12 or higher
- Kaggle account and API credentials
- Supabase account with S3-compatible storage

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd cimol
```

2. Install dependencies using uv:
```bash
uv sync
```

## Configuration

Create a `.env` file in the project root with the following environment variables:

```env
# Supabase S3 Configuration
BUCKET_NAME=your-bucket-name
SUPABASE_ACCESS_ID=your-access-key-id
SUPABASE_ACCESS_KEY=your-secret-access-key
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_REGION=your-region
SUPABASE_BUCKET_NAME=your-bucket-name

# Kaggle API Configuration
KAGGLE_USERNAME=your-kaggle-username
KAGGLE_KEY=your-kaggle-api-key
```

### Getting Kaggle API Credentials

1. Go to [Kaggle Account Settings](https://www.kaggle.com/account)
2. Scroll down to the "API" section
3. Click "Create New API Token" to download `kaggle.json`
4. Extract the username and key from the JSON file

### Getting Supabase Credentials

1. Go to your Supabase project dashboard
2. Navigate to Settings > API
3. Find your project URL and anon/public key
4. For S3 storage, go to Storage settings to get your bucket configuration

## Usage

### Basic Usage

Run the ETL pipeline:

```bash
uv run main.py
```

### What the Pipeline Does

The script will:

1. **Validate Environment**: Check that all required environment variables are set
2. **Download Dataset**: Download the "Food Consumption in Indonesia" dataset from Kaggle
3. **Process Excel Files**: Read and combine all Excel files in the dataset
4. **Data Cleaning**: 
   - Normalize column names (replace spaces with underscores, remove parentheses)
   - Convert text to uppercase
   - Remove invisible characters
   - Add source file and processing date columns
5. **Save CSV**: Export the processed data to a timestamped CSV file
6. **Upload to Storage**: Upload the final CSV to your Supabase S3 bucket

### Output

The pipeline generates:
- **Log file**: `etl_pipeline_YYYYMMDD.log` with detailed execution logs
- **CSV file**: `outputs/FoodConsumptionInIndonesia_YYYYMMDD.csv` with processed data
- **Cloud storage**: The CSV file is uploaded to your configured S3 bucket

### Example Output Structure

The processed CSV will contain columns like:
- `SOURCE_FILE`: Original Excel filename
- `PROCESS_DATE`: Date when the data was processed
- Normalized column names from the original Excel files
- All text data converted to uppercase
- Cleaned data with invisible characters removed

## Project Structure

```
cimol/
├── main.py                 # Main ETL pipeline script
├── pyproject.toml         # Project dependencies
├── README.md              # This file
├── .env                   # Environment variables (create this)
├── datasets/              # Downloaded Kaggle datasets
├── outputs/               # Processed CSV files
└── logs/                  # Execution logs
```

## Dependencies

- **boto3**: AWS S3 client for cloud storage
- **pandas**: Data processing and manipulation
- **kaggle**: Kaggle API client
- **openpyxl**: Excel file reading
- **python-dotenv**: Environment variable management

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**: Ensure all required variables are set in your `.env` file
2. **Kaggle Authentication**: Verify your Kaggle API credentials are correct
3. **S3 Upload Failures**: Check your Supabase S3 configuration and permissions
4. **Excel Reading Errors**: Ensure the downloaded files are valid Excel files

### Logs

Check the log file `etl_pipeline_YYYYMMDD.log` for detailed execution information and error messages.

