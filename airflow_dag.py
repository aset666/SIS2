"" 
"""
Apache Airflow DAG for Flashscore Football Data Pipeline.
ETL process: Scraping -> Cleaning -> Loading to SQLite.
Compatible with Apache Airflow 2.x.
"""
import os
import sys
from datetime import timedelta, datetime 
from pathlib import Path
from typing import Dict, Any, Union

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# Set project root directory to allow module imports
PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

# Import custom modules
try:
    from src.scraper import FlashscoreScraper
    from src.cleaner import MatchDataCleaner
    from src.loader import SQLiteLoader
except ImportError as e:
    print(f"MODULE IMPORT ERROR: {e}")


# DAG constants
DEFAULT_ARGS: Dict[str, Any] = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Define data paths
DB_PATH = PROJECT_ROOT / "data" / "output.db"
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw.json"
CLEANED_DATA_PATH = PROJECT_ROOT / "data" / "cleaned.json"


# Task functions

def scrape_flashscore_data(**context: Dict[str, Any]) -> int:
    """
    Task 1 (Extract): Scrape football match data.
    
    Returns and sends to XCom the count of collected records.
    """
    print("--- TASK 1: Starting Flashscore scraping ---")
    
    # Initialize scraper
    scraper = FlashscoreScraper(headless=True)
    matches = scraper.scrape(output_path=str(RAW_DATA_PATH))
    
    scraped_count = len(matches)
    print(f"Scraping completed: {scraped_count} matches extracted.")
    
    # Send data to XCom using task instance
    context['ti'].xcom_push(key='scraped_count', value=scraped_count)
    
    if scraped_count == 0:
        print("WARNING: Scraper returned no data.")
        
    return scraped_count


def clean_match_data(**context: Dict[str, Any]) -> int:
    """
    Task 2 (Transform): Clean and normalize collected data.
    
    Gets the count of collected records and sends the count of cleaned records.
    """
    print("--- TASK 2: Starting data cleaning ---")
    
    # Get record count from previous task
    scraped_count = context['ti'].xcom_pull(
        task_ids='scrape_data', 
        key='scraped_count',
        default=0
    )
    print(f"Received {scraped_count} raw records.")
    
    if scraped_count == 0:
        print("Skipping cleaning: No raw data available.")
        return 0
    
    # Initialize and run cleaner
    cleaner = MatchDataCleaner()
    cleaned_matches = cleaner.process(
        input_path=str(RAW_DATA_PATH),
        output_path=str(CLEANED_DATA_PATH)
    )
    
    cleaned_count = len(cleaned_matches)
    print(f"Cleaning completed: {cleaned_count} clean records.")
    print(f"Retention rate: {(cleaned_count/scraped_count*100):.1f}%")
    
    # Send data to XCom
    context['ti'].xcom_push(key='cleaned_count', value=cleaned_count)
    
    return cleaned_count


def load_to_sqlite(**context: Dict[str, Any]) -> Union[Dict[str, Any], None]:
    """
    Task 3 (Load): Load cleaned data into SQLite database.
    
    Gets the count of cleaned records for logging.
    """
    print("--- TASK 3: Starting database loading ---")
    
    # Get count of clean records from previous task
    cleaned_count = context['ti'].xcom_pull(
        task_ids='clean_data', 
        key='cleaned_count',
        default=0
    )
    print(f"Received {cleaned_count} clean records for loading.")
    
    if cleaned_count == 0:
        print("Skipping loading: No clean data available.")
        return None
        
    # Initialize loader
    loader = SQLiteLoader(db_path=str(DB_PATH))
    stats = loader.load(input_path=str(CLEANED_DATA_PATH))
    
    print("Loading completed successfully.")
    print(f"Total matches in database: {stats.get('total_matches', 0)}")
    
    # Send final stats to XCom
    context['ti'].xcom_push(key='db_stats', value=stats)
        
    return stats


# Define DAG

with DAG(
    dag_id='flashscore_football_pipeline',
    default_args=DEFAULT_ARGS,
    description='Daily ETL pipeline for collecting, cleaning, and loading Flashscore football match data',
    schedule='@daily',  # Updated for Airflow 2.x
    start_date=datetime(2025, 1, 1), 
    catchup=False,
    tags=['flashscore', 'football', 'etl'],
    max_active_runs=1,
) as dag:
    # Task 1: Scrape data
    scrape_task = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_flashscore_data,
    )

    # Task 2: Clean data
    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_match_data,
    )

    # Task 3: Load to SQLite
    load_task = PythonOperator(
        task_id='load_to_sqlite',
        python_callable=load_to_sqlite,
    )

    # Define task dependencies: E -> T -> L
    scrape_task >> clean_task >> load_task

    # DAG documentation in Markdown
    dag.doc_md = """
# Flashscore Football Match Data Pipeline

## Overview
This DAG implements an **ETL pipeline** for processing football match data from Flashscore. The pipeline consists of three sequential stages: Extract, Transform, and Load.


[Image of a data pipeline diagram showing three stages: Extract -> Transform -> Load]


## Pipeline Stages
1.  **`scrape_data`**: Uses **Selenium** to extract raw data.
2.  **`clean_data`**: Performs cleaning, validation, and normalization of data.
3.  **`load_to_sqlite`**: Loads the final clean data into an **SQLite** database.
"""
