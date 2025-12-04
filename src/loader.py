"" 
"""
Data Loader
Loads cleaned football match data into a SQLite database.
"""

import json
import logging
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MatchDataLoader:
    """
    Loads cleaned match data into a SQLite database.
    """
    def __init__(self, db_path: str = "football_matches.db"):
        """
        Initializes the loader with the database path.
        """
        self.db_path = Path(db_path)
        self.connection = None

    def connect(self):
        """
        Establishes a connection to the SQLite database.
        """
        try:
            self.connection = sqlite3.connect(self.db_path)
            # Use row_factory to get results as dictionaries (optional, useful for debugging)
            # self.connection.row_factory = sqlite3.Row 
            logger.info(f"Connected to SQLite database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to database {self.db_path}: {e}")
            raise

    def disconnect(self):
        """
        Closes the database connection.
        """
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from SQLite database.")

    def create_table(self):
        """
        Creates the 'matches' table in the database.
        """
        if not self.connection:
            raise RuntimeError("Database connection not established. Call connect() first.")

        logger.info("Creating 'matches' table if it doesn't exist...")

        # Define the table schema based on the fields from scraper and cleaner
        # Using TEXT for most fields, INTEGER for scores, and REAL for potential decimals if needed later
        # Using DATETIME for scraped_at and cleaned_at
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            scraped_at DATETIME,
            cleaned_at DATETIME,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            time_status TEXT,
            home_score INTEGER,
            away_score INTEGER,
            league TEXT,
            stage TEXT, -- e.g., FINISHED, SCHEDULED, LIVE
            match_status TEXT, -- e.g., Finished, Scheduled, Live (derived by cleaner)
            total_goals INTEGER,
            goal_difference INTEGER,
            is_draw BOOLEAN -- 1 for True, 0 for False
        );
        """

        try:
            cursor = self.connection.cursor()
            cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info("Table 'matches' created or already exists.")
        except sqlite3.Error as e:
            logger.error(f"Error creating table: {e}")
            self.connection.rollback()
            raise

    def load_data(self, input_json_path: str):
        """
        Loads data from a JSON file and inserts it into the 'matches' table.
        """
        if not self.connection:
            raise RuntimeError("Database connection not established. Call connect() first.")

        logger.info(f"Loading data from JSON file: {input_json_path}")

        # Load the cleaned JSON data
        try:
            with open(input_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            logger.error(f"Cleaned data file '{input_json_path}' not found.")
            return
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON file {input_json_path}: {e}")
            return

        if not data:
            logger.warning(f"No data found in {input_json_path} to load.")
            return

        logger.info(f"Loaded {len(data)} records from JSON.")

        # Prepare the INSERT statement using named placeholders (:column_name)
        insert_sql = """
        INSERT OR REPLACE INTO matches 
        (match_id, scraped_at, cleaned_at, home_team, away_team, time_status, 
         home_score, away_score, league, stage, match_status, total_goals, goal_difference, is_draw)
        VALUES 
        (:match_id, :scraped_at, :cleaned_at, :home_team, :away_team, :time_status,
         :home_score, :away_score, :league, :stage, :match_status, :total_goals, :goal_difference, :is_draw);
        """

        cursor = self.connection.cursor()
        successful_inserts = 0
        failed_inserts = 0

        for record in data:
            try:
                # Prepare the record for insertion
                # Handle boolean conversion for SQLite (True -> 1, False -> 0)
                processed_record = record.copy() # Work on a copy
                if 'is_draw' in processed_record:
                     processed_record['is_draw'] = 1 if processed_record['is_draw'] else 0
                
                # Handle potential None values for fields expected to be integers in the DB schema
                # These should ideally be handled by the cleaner, but ensure here too.
                for field in ['home_score', 'away_score', 'total_goals', 'goal_difference']:
                    if processed_record.get(field) is None or processed_record[field] == '': # Also check for empty string if cleaner didn't handle it strictly
                        processed_record[field] = None # SQLite will store this as NULL
                        
                cursor.execute(insert_sql, processed_record)
                successful_inserts += 1
            except sqlite3.Error as e:
                logger.error(f"Error inserting record {record.get('match_id', 'N/A')}: {e}")
                logger.debug(f"Problematic record data: {record}") # Log the record causing the error for debugging
                failed_inserts += 1
                # Continue processing other records even if one fails
            except KeyError as e:
                logger.error(f"Missing key in record {record.get('match_id', 'N/A')}: {e}")
                logger.debug(f"Record data: {record}")
                failed_inserts += 1
                # Continue processing other records even if one fails

        # Commit all successful inserts after processing the loop
        try:
            self.connection.commit()
            logger.info(f"Data loading completed. Successfully inserted: {successful_inserts}, Failed: {failed_inserts}.")
        except sqlite3.Error as e:
            logger.error(f"Error committing transactions: {e}")
            self.connection.rollback()
            # Re-raise to indicate failure
            raise


    def run(self, input_json_path: str):
        """
        Executes the full loading process: connect, create table, load data, disconnect.
        """
        logger.info("Starting data loading process...")
        try:
            self.connect()
            self.create_table()
            self.load_data(input_json_path)
        finally:
            self.disconnect()
        logger.info("Data loading process finished.")


if __name__ == "__main__":
    INPUT_FILE = "test_cleaned.json" # The output file from cleaner.py
    DB_FILE = "football_matches.db"  # The SQLite database file to create/load into

    print(f"Testing Match Data Loader...")
    loader = MatchDataLoader(db_path=DB_FILE)

    if not Path(INPUT_FILE).exists():
        logger.error(f"Cleaned data file '{INPUT_FILE}' not found.")
        logger.error(f"Please run 'python cleaner.py' first to generate the cleaned data.")
    else:
        loader.run(INPUT_FILE) # Передаем путь к файлу как позиционный аргумент
        print(f"\nTest complete: Data loaded from {INPUT_FILE} into {DB_FILE}")
