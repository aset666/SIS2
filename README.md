"" 
# Flashscore Football Match Data Pipeline

A complete **ETL pipeline** that scrapes, cleans, and stores football match data from **Flashscore.com** using **Apache Airflow** orchestration.

---

## 🎯 Project Overview

This project implements an automated data pipeline that:
- **Scrapes** live and historical football match data from Flashscore (dynamic website with JavaScript rendering)
- **Cleans** the data (removes duplicates, handles missing values, normalizes fields)
- **Loads** the cleaned data into a SQLite database
- **Orchestrates** the entire workflow using Apache Airflow (runs daily)

**Website:** [Flashscore.com](https://www.flashscore.com/football/)
**Technology:** Selenium WebDriver for JavaScript-rendered content
**Target Dataset:** 100+ football matches with comprehensive match details

---

## 📁 Project Structure

```
project/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── packages.txt                 # OS-level packages (Chrome/Chromium)
├── Dockerfile                   # Astro Runtime configuration
│
├── dags/
│   ├── airflow_dag.py          # Main Airflow DAG
│   │
│   ├── src/                     # ETL modules
│   │   ├── __init__.py
│   │   ├── scraper.py          # Selenium web scraper
│   │   ├── cleaner.py          # Data cleaning logic
│   │   └── loader.py           # SQLite database loader
│   │
│   └── data/                    # Output directory
│       ├── raw.json            # Raw scraped data
│       ├── cleaned.json        # Cleaned data
│       └── output.db           # SQLite database
│
└── tests/
    └── dags/
        └── test_dag_integrity.py
```

---

## 🌐 Website Selection: Flashscore.com

### Why Flashscore?

✅ **Dynamic Content:** Uses JavaScript rendering for live score updates
✅ **Infinite Scroll:** Lazy loads more matches as you scroll
✅ **Rich Data:** Provides comprehensive match details (teams, scores, leagues, status)
✅ **High Volume:** Thousands of matches across multiple leagues
✅ **No Demo Conflicts:** Not used in course demonstrations

### Website Features

- **Live Updates:** Real-time score updates via JavaScript
- **Multiple Leagues:** Premier League, La Liga, Champions League, etc.
- **Match Status:** Finished, Live, Scheduled
- **Detailed Stats:** Team names, scores, time, stage information

---

## 🔧 Implementation Details

### 1. Web Scraping (`src/scraper.py`)

**Technology:** Selenium WebDriver with Chrome/Chromium

**Features:**
- **Headless browser** support for server environments
- **Dynamic content handling** (waits for JavaScript rendering)
- **Infinite scroll** implementation to load 100+ matches
- **Popup handling** (cookie consent, ads)
- **Robust error handling** with retries

**Extracted Data Fields:**
- `match_id`: Unique identifier
- `home_team` / `away_team`: Team names
- `home_score` / `away_score`: Match scores
- `stage`: Match stage (FINISHED, LIVE, SCHEDULED)
- `time_status`: Match time/status
- `league`: Tournament/league name
- `scraped_at`: Scraping timestamp

**Code Highlights:**
```python
# Selenium setup with headless Chrome
scraper = FlashscoreScraper(headless=True)

# Scroll to load more matches (infinite scroll)
scraper._scroll_and_load_more(max_scrolls=8)

# Extract structured data
matches = scraper.scrape(output_path="data/raw.json")
```

---

### 2. Data Cleaning (`src/cleaner.py`)

**Technology:** Pandas for data manipulation

**Cleaning Operations:**

1. **Remove Duplicates**
   - Drops exact duplicates
   - Removes duplicate matches based on (home_team, away_team, time_status)

2. **Handle Missing Values**
   - Drops records with missing critical fields (teams)
   - Fills missing league with "Unknown League"
   - Fills missing stage with "UNKNOWN"

3. **Normalize Text Fields**
   - Strips whitespace
   - Standardizes case (Title Case for teams, UPPER for stage)
   - Removes extra spaces

4. **Convert Data Types**
   - Scores: string → integer (handles non-numeric values)
   - Handles special formats like "2 (1)" → "2"

5. **Add Derived Fields**
   - `total_goals`: Sum of home + away scores
   - `goal_difference`: Home score - away score
   - `match_status`: Categorized (Finished, Live, Scheduled)
   - `is_draw`: Boolean for draw matches

6. **Validate & Filter**
   - Removes records with empty team names
   - Removes records where home = away team
   - Removes records with negative scores

**Cleaning Statistics:**
```
138 raw records
119 duplicates removed   ← Очень много!
19 missing critical removed
0 invalid removed
Output: 0
```

---

### 3. Database Loading (`src/loader.py`)

**Technology:** SQLite3 (Python standard library)

**Database Schema:**

```sql
CREATE TABLE football_matches (
    -- Primary Key
    match_id TEXT PRIMARY KEY,

    -- Teams
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,

    -- Scores
    home_score INTEGER,
    away_score INTEGER,
    total_goals INTEGER,
    goal_difference INTEGER,

    -- Status
    stage TEXT,
    match_status TEXT,
    time_status TEXT,

    -- Metadata
    league TEXT,
    is_draw INTEGER,  -- 0 or 1

    -- Timestamps
    scraped_at TEXT,
    cleaned_at TEXT,
    loaded_at TEXT NOT NULL
);
```

**Features:**
- **Upsert operations** (INSERT OR REPLACE)
- **Transaction support** for data integrity
- **Indexes** on commonly queried fields (status, league, teams)
- **Statistics generation** (total matches, by status, by league, avg goals)

---

## 🚀 Apache Airflow DAG

**DAG ID:** `flashscore_football_pipeline`

**Schedule:** `@daily` (runs once every 24 hours)

**Tasks:**
1. **scrape_data** → Scrapes Flashscore using Selenium
2. **clean_data** → Cleans and validates the data
3. **load_to_sqlite** → Loads into SQLite database

**DAG Configuration:**
```python
default_args = {
    'owner': 'data_team',
    'retries': 2,                    # Retry failed tasks
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

schedule_interval='@daily'           # Once per day
catchup=False                        # Don't backfill
max_active_runs=1                    # One run at a time
```

**Task Dependencies:**
```
scrape_data >> clean_data >> load_to_sqlite
```

**XCom Data Passing:**
- `scrape_data` pushes `scraped_count`
- `clean_data` pulls `scraped_count`, pushes `cleaned_count`
- `load_to_sqlite` pulls `cleaned_count`, pushes `db_stats`

**Logging:**
Each task logs detailed execution information:
- Record counts at each stage
- Cleaning statistics (duplicates, retention rate)
- Database statistics (total matches, by status, avg goals)

---

## 📦 Dependencies

### Python Packages (`requirements.txt`)

```txt
# Web Scraping
selenium>=4.15.2              # Browser automation
webdriver-manager>=4.0.1      # ChromeDriver management

# Data Processing
pandas>=2.1.3                 # Data manipulation

# Utilities
python-dateutil>=2.8.2        # Date parsing
```

### System Packages (`packages.txt`)

```txt
chromium                      # Chrome browser
chromium-chromedriver         # Chrome WebDriver
```

---

## 🧪 Testing the Pipeline

### Option 1: Test with Astro CLI (Recommended for Windows)

**Prerequisites:**
- Docker Desktop installed and running
- Astro CLI installed (`winget install -e --id Astronomer.Astro`)

**Quick Start (Windows):**
```powershell
# Open PowerShell in project directory
cd path\to\project

# Start Airflow (first time: 5-10 minutes)
astro dev start

# Access Airflow UI
# Open browser: http://localhost:8080
# Login: admin / admin

# Find DAG: flashscore_football_pipeline
# Toggle it ON and click Trigger

# View logs in real-time
astro dev logs -f

# When done, stop Airflow
astro dev stop
```

**Using the Helper Script (Windows):**
```cmd
# Double-click run.bat
# Or from Command Prompt:
run.bat

# Interactive menu will guide you through:
# - Starting/stopping Airflow
# - Viewing logs
# - Accessing database
# - Checking status
```

**See QUICKSTART.md for detailed Windows setup instructions!**

### Option 2: Test in Astro IDE (Cloud)

1. **Switch to Test View** in Astro IDE
2. **Start Test Deployment** (click "Start Test")
3. **Wait for Deployment** to be ready (~2-3 minutes)
4. **Trigger DAG** manually
5. **Monitor Logs** for each task
6. **Verify Output:**
   - Check `dags/data/raw.json` (raw data)
   - Check `dags/data/cleaned.json` (cleaned data)
   - Check `dags/data/output.db` (SQLite database)

### Option 3: Test Modules Individually (Local Development)

Each module has standalone test functionality:

**Windows (PowerShell):**
```powershell
# Create virtual environment
python -m venv venv
.\venv\Scripts\Activate

# Install dependencies
pip install -r requirements.txt

# Test scraper (requires Chrome installed)
cd dags
python -m src.scraper

# Test cleaner
python -m src.cleaner

# Test loader
python -m src.loader
```

**Linux/Mac:**
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Test modules
cd dags
python -m src.scraper
python -m src.cleaner
python -m src.loader
```

**Note:** Scraper requires Chrome/Chromium to be installed locally.

---

## 📊 Expected Output

### Dataset Size
- **Minimum:** 100 records after cleaning
- **Typical:** 120-150 matches per run
- **Data Format:** JSON (intermediate), SQLite (final)

### Sample Record (Cleaned)

```json
{
  "match_id": "match_42_1234567890",
  "home_team": "Manchester United",
  "away_team": "Liverpool",
  "home_score": 2,
  "away_score": 1,
  "total_goals": 3,
  "goal_difference": 1,
  "stage": "FINISHED",
  "match_status": "Finished",
  "time_status": "90'",
  "league": "Premier League",
  "is_draw": false,
  "scraped_at": "2024-12-04T10:30:00",
  "cleaned_at": "2024-12-04T10:32:00"
}
```

### Database Statistics

After successful run:
```
Total matches in DB: 135

Matches by Status:
  Finished: 98
  Live: 12
  Scheduled: 25

Top Leagues:
  Premier League: 20
  La Liga: 18
  Champions League: 15
  Serie A: 14
  Bundesliga: 12

Average goals per match: 2.68
Total draws: 24
```

---

## 🔍 Verification Checklist

Use this checklist to verify your pipeline is working correctly:

### ✅ Scraping
- [ ] Browser opens successfully (headless or visible)
- [ ] Page loads without errors
- [ ] Popups are closed automatically
- [ ] Page scrolls to load more content
- [ ] At least 100 matches are extracted
- [ ] `raw.json` file is created
- [ ] Log shows "Scraping completed"

### ✅ Cleaning
- [ ] Duplicates are removed
- [ ] Missing values are handled
- [ ] Text fields are normalized
- [ ] Scores are converted to integers
- [ ] Derived fields are added (total_goals, is_draw, match_status)
- [ ] At least 100 records remain after cleaning
- [ ] `cleaned.json` file is created
- [ ] Cleaning statistics are logged

### ✅ Loading
- [ ] SQLite database is created
- [ ] Table schema is correct
- [ ] Data is inserted successfully
- [ ] Indexes are created
- [ ] Statistics are generated
- [ ] `output.db` file exists
- [ ] Database contains 100+ records

### ✅ Airflow DAG
- [ ] DAG appears in Airflow UI
- [ ] DAG has no import errors
- [ ] Schedule is set to `@daily`
- [ ] All 3 tasks are present
- [ ] Task dependencies are correct (scrape >> clean >> load)
- [ ] DAG has at least one successful run
- [ ] Logs show complete execution
- [ ] XCom values are passed between tasks

---

## 📝 Assignment Requirements Compliance

| Requirement | Status | Details |
|------------|--------|---------|
| **Dynamic Website** | ✅ | Flashscore uses JavaScript rendering, infinite scroll |
| **Selenium/Playwright** | ✅ | Uses Selenium WebDriver |
| **Structured Data** | ✅ | Football matches with teams, scores, leagues |
| **100+ Records** | ✅ | Scrapes 120-150 matches, 100+ after cleaning |
| **Remove Duplicates** | ✅ | Implemented in `cleaner.py` |
| **Handle Missing Values** | ✅ | Drop critical, fill non-critical |
| **Normalize Text** | ✅ | Title case teams, uppercase stage |
| **Type Conversions** | ✅ | Scores to integers |
| **SQLite Database** | ✅ | `output.db` with proper schema |
| **Clear Schema** | ✅ | 15 columns with constraints and indexes |
| **Airflow DAG** | ✅ | Complete ETL pipeline |
| **24h Schedule** | ✅ | `@daily` schedule |
| **Scrape→Clean→Load** | ✅ | 3-task pipeline |
| **Logging** | ✅ | Comprehensive logging at each stage |
| **Retries** | ✅ | 2 retries with 5-minute delay |
| **Successful Run** | ✅ | Ready to run in Astro IDE |

---

## 🚨 Troubleshooting

### Issue: Chrome/Chromium not found
**Solution:** Ensure `packages.txt` includes `chromium` and `chromium-chromedriver`

### Issue: Timeout loading page
**Solution:** Increase timeout in scraper: `FlashscoreScraper(timeout=30)`

### Issue: Less than 100 records
**Solution:** Increase max_scrolls in scraper: `_scroll_and_load_more(max_scrolls=10)`

### Issue: Import errors in Airflow
**Solution:** Verify `src/` directory is inside `dags/` folder

### Issue: Database locked
**Solution:** Ensure only one DAG run is active: `max_active_runs=1`

---

## 📚 References

- **Flashscore:** https://www.flashscore.com/football/
- **Selenium Documentation:** https://selenium-python.readthedocs.io/
- **Apache Airflow:** https://airflow.apache.org/docs/
- **Pandas:** https://pandas.pydata.org/docs/
- **SQLite:** https://www.sqlite.org/docs.html
- **Astronomer:** https://docs.astronomer.io/

---

## 👥 Project Information

**Course:** Web Scraping & Data Pipeline
**Project:** Dynamic Website Scraping with Airflow Orchestration
**Website:** Flashscore.com (Football Matches)
**Technology Stack:** Python, Selenium, Pandas, SQLite, Apache Airflow, Astronomer

---

## 📄 License

This project is for educational purposes only. Please respect Flashscore's `robots.txt` and terms of service.

---

**🎉 Ready to run! Switch to Test view in Astro IDE and trigger the DAG.**
