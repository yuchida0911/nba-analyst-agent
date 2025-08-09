# NBA Analyst Agent - Usage Guide

This guide covers how to use the NBA Analyst Agent data ingestion pipeline for importing and processing NBA statistical data.

## Table of Contents
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Data Ingestion](#data-ingestion)
- [Database Setup](#database-setup)
- [Configuration](#configuration)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## Installation

### Prerequisites
- Python 3.9 or higher
- PostgreSQL 13+ (for production) or SQLite (for testing)
- 8GB RAM minimum, 4 CPU cores recommended

### Setup
1. Clone the repository:
```bash
git clone <repository-url>
cd nba-analyst-agent
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your database credentials
```

## Quick Start

### 1. Database Setup
Create database tables:
```bash
python scripts/create_database.py
```

### 2. Ingest Sample Data
```python
from analytics_pipeline.ingestion.ingest import create_ingestion_pipeline

# Create ingestion pipeline
pipeline = create_ingestion_pipeline(
    batch_size=1000,
    validate_data=True,
    upsert_mode=True
)

# Ingest box scores (player-level data)
result = pipeline.ingest_csv_file(
    file_path="NBA-Data-2010-2024/regular_season_box_scores_2010_2024_part_1.csv",
    data_type="box_scores"
)

print(f"Success: {result.success}")
print(f"Rows ingested: {result.stats.rows_inserted}")
```

## Data Ingestion

### Supported Data Types

#### Box Scores (Player-Level Statistics)
- **File Type**: `box_scores`
- **Table**: `players_raw`
- **Contains**: Individual player performance data per game
- **Key Fields**: gameId, personId, points, assists, rebounds, etc.

#### Totals (Team-Level Statistics)
- **File Type**: `totals`
- **Table**: `teams_raw`
- **Contains**: Team performance data and rankings per game
- **Key Fields**: GAME_ID, TEAM_ID, PTS, WL, etc.

### Ingestion Pipeline Features

#### Data Validation
- **Business Rules**: Shooting consistency, rebounds calculation, non-negative stats
- **Format Validation**: Season year format, team codes, player positions
- **Severity Levels**: INFO, WARNING, ERROR, CRITICAL

#### Batch Processing
- Configurable batch sizes (default: 1000 rows)
- Memory-efficient processing for large datasets
- Progress tracking and statistics

#### Error Handling
- Graceful handling of malformed data
- Detailed error reporting with row-level context
- Configurable error tolerance levels

### Basic Usage Examples

#### Simple Ingestion
```python
from analytics_pipeline.ingestion.ingest import create_ingestion_pipeline

pipeline = create_ingestion_pipeline()

# Ingest a single file
result = pipeline.ingest_csv_file("data/box_scores.csv")

if result.success:
    print(f"Successfully ingested {result.stats.rows_inserted} rows")
else:
    print(f"Ingestion failed: {result.errors}")
```

#### Custom Configuration
```python
from analytics_pipeline.ingestion.ingest import NBADataIngestion
from analytics_pipeline.database.connection import get_database_connection

# Custom database connection
db_conn = get_database_connection()

# Custom pipeline with specific settings
pipeline = NBADataIngestion(
    db_connection=db_conn,
    batch_size=500,           # Smaller batches for limited memory
    validate_data=True,       # Enable data validation
    upsert_mode=True         # Handle duplicate records
)

result = pipeline.ingest_csv_file(
    file_path="data/large_dataset.csv",
    max_rows=10000           # Limit for testing
)
```

#### Multiple File Processing
```python
import glob
from pathlib import Path

pipeline = create_ingestion_pipeline()
results = []

# Process all box score files
for file_path in glob.glob("NBA-Data-2010-2024/*box_scores*.csv"):
    result = pipeline.ingest_csv_file(file_path, data_type="box_scores")
    results.append(result)
    
    print(f"Processed {Path(file_path).name}: {result.stats.rows_inserted} rows")

# Generate summary
summary = pipeline.get_ingestion_summary(results)
print(f"Total files: {summary['files_processed']}")
print(f"Success rate: {summary['success_rate']:.2%}")
print(f"Total rows: {summary['total_rows_inserted']}")
```

## Database Setup

### PostgreSQL (Production)
1. Install PostgreSQL 13+
2. Create database and user:
```sql
CREATE DATABASE nba_analyst;
CREATE USER nba_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE nba_analyst TO nba_user;
```

3. Update `.env` file:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nba_analyst
DB_USER=nba_user
DB_PASSWORD=your_secure_password
```

4. Create tables:
```bash
python scripts/create_database.py
```

### SQLite (Development/Testing)
For development or testing, you can use SQLite:
```
TESTING=true
DB_HOST=
DB_NAME=nba_analyst.db
DB_USER=
DB_PASSWORD=
```

## Configuration

### Environment Variables
The system uses environment variables for configuration. Copy `.env.example` to `.env` and customize:

#### Database Settings
```
DB_HOST=localhost          # Database host
DB_PORT=5432              # Database port
DB_NAME=nba_analyst       # Database name
DB_USER=nba_user          # Database user
DB_PASSWORD=password      # Database password
DB_SCHEMA=public          # Database schema
```

#### Processing Settings
```
BATCH_SIZE=1000           # Default batch size
MAX_WORKERS=4             # Maximum worker threads
DATA_VALIDATION_STRICT=false  # Strict validation mode
MAX_ERROR_RATE=0.01       # Maximum acceptable error rate
```

#### Logging Settings
```
LOG_LEVEL=INFO            # Logging level
LOG_FILE=./logs/analytics_pipeline.log  # Log file path
```

### Programmatic Configuration
```python
from analytics_pipeline.config.settings import Settings

# Create custom settings
settings = Settings(
    db_host="localhost",
    db_name="nba_analyst",
    batch_size=2000,
    log_level="DEBUG",
    validate_data=True
)
```

## Testing

### Running Tests
```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit/

# Run integration tests only
pytest tests/integration/

# Run with coverage
pytest --cov=src/nba_analyst

# Run specific test class
pytest tests/unit/test_ingestion.py::TestNBADataIngestion -v
```

### Test Configuration
Tests use SQLite in-memory databases for speed and isolation. No additional setup required.

### Test Categories
- **Unit Tests**: Individual components (64 tests)
- **Integration Tests**: End-to-end workflows (7 tests)
- **Database Tests**: Model and database interactions

## Troubleshooting

### Common Issues

#### Import Errors
```
ModuleNotFoundError: No module named 'nba_analyst'
```
**Solution**: Ensure you're in the project root and Python path is correct:
```python
import sys
sys.path.insert(0, 'src')
```

#### Database Connection Errors
```
psycopg2.OperationalError: connection to server failed
```
**Solutions**:
1. Verify PostgreSQL is running
2. Check database credentials in `.env`
3. Ensure database and user exist
4. Test with SQLite for development: `TESTING=true`

#### Memory Issues with Large Files
```
MemoryError: Unable to allocate array
```
**Solutions**:
1. Reduce batch size: `batch_size=500`
2. Process files in chunks using `max_rows` parameter
3. Increase system memory
4. Use streaming processing for very large files

#### Validation Errors
```
Too many validation errors: 150
```
**Solutions**:
1. Check data quality in source CSV files
2. Increase error tolerance: `max_errors=500`
3. Disable strict validation: `strict_mode=False`
4. Review validation rules in logs

### Performance Optimization

#### Large Dataset Processing
```python
# For files with millions of rows
pipeline = create_ingestion_pipeline(
    batch_size=5000,          # Larger batches for efficiency
    validate_data=False,      # Skip validation for speed
    upsert_mode=False        # Use simple insert if no duplicates
)
```

#### Memory-Efficient Processing
```python
# Process large files in chunks
import math

file_size = 1000000  # Total rows
chunk_size = 50000   # Process 50k rows at a time
chunks = math.ceil(file_size / chunk_size)

for i in range(chunks):
    start_row = i * chunk_size
    result = pipeline.ingest_csv_file(
        file_path="large_file.csv",
        max_rows=chunk_size,
        # Note: This is a conceptual example
        # Actual implementation would need file seeking
    )
```

## API Reference

### Main Classes

#### `NBADataIngestion`
Primary ingestion pipeline class.
- `ingest_csv_file(file_path, data_type=None, max_rows=None)`: Ingest single CSV file
- `get_ingestion_summary(results)`: Generate summary from multiple results

#### `IngestionResult`
Result object returned by ingestion operations.
- `success`: Boolean indicating success/failure
- `stats`: Detailed statistics (`IngestionStats`)
- `errors`: List of error messages
- `file_path`: Path to processed file

#### `IngestionStats`
Detailed statistics from ingestion.
- `total_rows_read`: Total rows read from CSV
- `rows_inserted`: Rows successfully inserted
- `rows_updated`: Rows updated (in upsert mode)
- `validation_errors`: Number of validation errors
- `success_rate`: Percentage of successful rows

### Factory Functions

#### `create_ingestion_pipeline(**kwargs)`
Create configured ingestion pipeline.
```python
pipeline = create_ingestion_pipeline(
    batch_size=1000,
    validate_data=True,
    upsert_mode=True
)
```

#### `create_csv_reader(**kwargs)`
Create configured CSV reader.
```python
reader = create_csv_reader(
    chunk_size=1000,
    validate_data=True,
    strict_mode=False
)
```

#### `create_validator(**kwargs)`
Create configured data validator.
```python
validator = create_validator(
    strict_mode=False,
    max_errors=100
)
```

---

For additional help, see the [PRD document](PRD.md) for technical specifications or check the test files for usage examples.