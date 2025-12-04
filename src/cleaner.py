"" 

import json
import logging
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MatchCleaner")


class MatchDataCleaner:
    """Clean and preprocess football match data."""

    def __init__(self):
        self.stats = {
            "input_count": 0,
            "duplicates_removed": 0,
            "invalid_removed": 0,
            "output_count": 0,
        }

    # --------------------------------------------------------------
    # Public API
    # --------------------------------------------------------------
    def process(self, input_path: str, output_path: str) -> List[Dict[str, Any]]:
        """Main cleaning pipeline."""
        logger.info("=" * 60)
        logger.info("Starting Match Data Cleaning...")
        logger.info("=" * 60)

        data = self._load_json(input_path)
        if data is None:
            return []

        self.stats["input_count"] = len(data)
        df = pd.DataFrame(data)
        logger.info(f"Loaded {len(df)} raw records")

        if df.empty:
            logger.warning("No data to clean!")
            self._save_json([], output_path)
            return []

        df = self._remove_duplicates(df)
        df = self._handle_missing_values(df)
        df = self._normalize_text_fields(df)
        df = self._convert_data_types(df)
        df = self._add_derived_fields(df)
        df = self._validate_and_filter(df)

        cleaned = df.to_dict("records")
        self.stats["output_count"] = len(cleaned)

        self._save_json(cleaned, output_path)
        self._print_stats()

        return cleaned

    # --------------------------------------------------------------
    # Utility
    # --------------------------------------------------------------
    def _load_json(self, path: str):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load JSON: {e}")
            return None

    def _save_json(self, data: List[Dict], path: str):
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save JSON: {e}")

    # --------------------------------------------------------------
    # Cleaning Steps
    # --------------------------------------------------------------
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove exact and logical duplicates."""
        before = len(df)

        df = df.drop_duplicates()

        if {"home_team", "away_team", "time_status"}.issubset(df.columns):
            df = df.drop_duplicates(
                subset=["home_team", "away_team", "time_status"], keep="first"
            )

        self.stats["duplicates_removed"] = before - len(df)
        logger.info(f"Removed {self.stats['duplicates_removed']} duplicates")
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove records with missing critical fields."""
        before = len(df)

        # Drop rows missing team names
        critical = ["home_team", "away_team"]
        df = df.dropna(subset=critical)

        # Fill non-critical fields
        df["league"] = df.get("league", "Unknown League").fillna("Unknown League")
        df["stage"] = df.get("stage", "UNKNOWN").fillna("UNKNOWN")

        removed = before - len(df)
        self.stats["invalid_removed"] += removed
        logger.info(f"Removed {removed} records with missing critical fields")
        return df

    def _normalize_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize formatting of text fields."""
        text_fields = ["home_team", "away_team", "league", "stage"]

        for col in text_fields:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.replace(r"\s+", " ", regex=True)
                )

                if "team" in col:
                    df[col] = df[col].str.title()

                if col == "stage":
                    df[col] = df[col].str.upper()

        logger.info("Normalized text fields")
        return df

    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert score fields and dates."""
        for col in ["home_score", "away_score"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.extract(r"(\d+)", expand=False)
                )
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(-1).astype(int)

        logger.info("Converted score types")
        return df

    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add useful columns for analysis."""
        if {"home_score", "away_score"}.issubset(df.columns):
            df["total_goals"] = df["home_score"] + df["away_score"]
            df["goal_difference"] = df["home_score"] - df["away_score"]
            df["is_draw"] = df["home_score"] == df["away_score"]

        # Match status classification
        def classify(stage):
            stage = str(stage).upper()
            if stage in ("FINISHED", "ENDED", "FT"):
                return "Finished"
            if stage in ("LIVE", "INPLAY"):
                return "Live"
            return "Scheduled"

        df["match_status"] = df.get("stage", "").apply(classify)

        logger.info("Added derived fields")
        return df

    def _validate_and_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove logically invalid records."""
        before = len(df)

        # Remove identical teams
        df = df[df["home_team"] != df["away_team"]]

        # Remove negative scores
        df = df[(df["home_score"] >= 0) & (df["away_score"] >= 0)]

        # Remove empty names after normalization
        df = df[(df["home_team"] != "") & (df["away_team"] != "")]

        removed = before - len(df)
        self.stats["invalid_removed"] += removed
        logger.info(f"Removed {removed} invalid records")
        return df

    # --------------------------------------------------------------
    # Stats
    # --------------------------------------------------------------
    def _print_stats(self):
        logger.info("\n" + "=" * 50)
        logger.info("CLEANING STATISTICS")
        logger.info("=" * 50)
        logger.info(f"Input records:       {self.stats['input_count']}")
        logger.info(f"Duplicates removed:  {self.stats['duplicates_removed']}")
        logger.info(f"Invalid removed:     {self.stats['invalid_removed']}")
        logger.info(f"Output records:      {self.stats['output_count']}")

        if self.stats["input_count"]:
            rate = (self.stats["output_count"] / self.stats["input_count"]) * 100
            logger.info(f"Retention rate:      {rate:.1f}%")
        logger.info("=" * 50)


if __name__ == "__main__":
    cleaner = MatchDataCleaner()
    cleaner.process("test_raw.json", "test_cleaned.json")
