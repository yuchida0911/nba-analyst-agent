# NBA Analyst Agent - Complete Setup Instructions

Follow these steps to set up PostgreSQL and load your NBA datasets with complete advanced analytics processing.

## 📋 Prerequisites

Make sure you have:
- ✅ Python 3.9+ installed
- ✅ NBA-Data-2010-2024/ directory with CSV files
- ✅ Virtual environment activated: `source venv/bin/activate`

## 🗄️ Step 1: Install and Setup PostgreSQL

### On macOS (Homebrew):
```bash
# Install PostgreSQL
brew install postgresql@15

# Start PostgreSQL service
brew services start postgresql@15

# Verify it's running
brew services list | grep postgresql
```

### On Ubuntu/Linux:
```bash
# Install PostgreSQL
sudo apt update
sudo apt install postgresql postgresql-contrib

# Start PostgreSQL
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

## 🔧 Step 2: Create Database and User

Connect to PostgreSQL as superuser:
```bash
# On macOS (usually no password needed)
psql postgres

# On Linux (may need to switch to postgres user)
sudo -u postgres psql
```

Run these SQL commands (copy and paste all at once):
```sql
-- Create database and user
CREATE DATABASE nba_analyst;
CREATE USER nba_user WITH PASSWORD 'nba_secure_2024';
GRANT ALL PRIVILEGES ON DATABASE nba_analyst TO nba_user;

-- Connect to new database and set permissions
\c nba_analyst

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO nba_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO nba_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO nba_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO nba_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO nba_user;

-- Verify setup
\du
\l

-- Exit PostgreSQL
\q
```

## ⚙️ Step 3: Configure Environment

Update your `.env` file with PostgreSQL credentials:
```bash
# Check your current .env file
cat .env
```

Make sure it contains:
```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nba_analyst
DB_USER=nba_user
DB_PASSWORD=nba_secure_2024
DB_SCHEMA=public

# Processing Configuration  
BATCH_SIZE=1000
LOG_LEVEL=INFO
DEBUG=false
TESTING=false
```

## 🏗️ Step 4: Create Database Schema

Create all required tables and indexes:
```bash
source venv/bin/activate
python scripts/setup_postgres.py
```

**Expected Output:**
```
🏀 NBA Analyst Agent - PostgreSQL Database Setup
✅ .env file found
✅ Configuration loaded successfully
✅ Database connection successful
✅ Database schema created successfully!
📊 Tables created: 4
   • players_raw (6 indexes)
   • teams_raw (4 indexes)  
   • players_processed (7 indexes)
   • player_monthly_trends (4 indexes)
```

## 📊 Step 5: Load NBA Data

Load your complete NBA dataset with advanced processing:
```bash
source venv/bin/activate
python scripts/load_nba_data.py
```

This will:
1. **Discover CSV files** in NBA-Data-2010-2024/
2. **Ingest raw data** into players_raw and teams_raw tables
3. **Calculate advanced metrics** (TS%, PER, Defensive Impact, etc.)
4. **Generate AI-ready data** in players_processed table

**Expected Processing:**
- `regular_season_box_scores_2010_2024_part_1.csv`
- `regular_season_box_scores_2010_2024_part_2.csv`  
- `regular_season_box_scores_2010_2024_part_3.csv`
- `regular_season_totals_2010_2024.csv`
- `play_off_box_scores_2010_2024.csv`
- `play_off_totals_2010_2024.csv`

**Expected Results:**
- ~500K+ player game records with advanced metrics
- ~50K+ team game records
- Complete processing in 10-30 minutes (depending on hardware)

## ✅ Step 6: Verify Your Data

Test that everything loaded correctly:

```bash
# Connect to your database
psql -h localhost -U nba_user -d nba_analyst
```

```sql
-- Check record counts
SELECT 'players_raw' as table_name, COUNT(*) as records FROM players_raw
UNION ALL
SELECT 'teams_raw', COUNT(*) FROM teams_raw
UNION ALL  
SELECT 'players_processed', COUNT(*) FROM players_processed;

-- Sample advanced metrics data
SELECT 
    person_name,
    season_year,
    game_date,
    points,
    minutes_played,
    true_shooting_pct,
    player_efficiency_rating,
    defensive_impact_score,
    efficiency_grade,
    defensive_grade
FROM players_processed 
WHERE person_name ILIKE '%lebron%'
    AND season_year = '2023-24'
    AND minutes_played > 20
ORDER BY game_date DESC
LIMIT 5;

-- Top performers by True Shooting %
SELECT 
    person_name,
    COUNT(*) as games,
    ROUND(AVG(true_shooting_pct), 3) as avg_ts_pct,
    ROUND(AVG(points), 1) as avg_points
FROM players_processed 
WHERE minutes_played >= 20 
    AND season_year = '2023-24'
GROUP BY person_name
HAVING COUNT(*) >= 10
ORDER BY avg_ts_pct DESC
LIMIT 10;
```

## 🚨 Troubleshooting

### Database Connection Issues:
```bash
# Check if PostgreSQL is running
brew services list | grep postgresql
# or on Linux:
sudo systemctl status postgresql

# Restart if needed
brew services restart postgresql@15
# or on Linux:
sudo systemctl restart postgresql

# Test connection manually
psql -h localhost -U nba_user -d nba_analyst
```

### Permission Issues:
```bash
# Show exact SQL commands for your setup
python scripts/setup_postgres.py --create-user

# Reset database if needed
python scripts/setup_postgres.py --reset
```

### Data Loading Issues:
```bash
# Check logs for detailed error information
tail -f logs/nba_data_loading.log

# Load with debug information
python scripts/load_nba_data.py --log-level DEBUG

# Use smaller batch size if memory issues
python scripts/load_nba_data.py --batch-size 500
```

### Environment Issues:
```bash
# Make sure virtual environment is activated
source venv/bin/activate

# Check Python path
which python

# Reinstall requirements if needed
pip install -r requirements.txt
```

## 🎯 Success! Next Steps

Once your data is loaded successfully, you can:

1. **Query Advanced Metrics:**
   ```sql
   -- Players with best efficiency grades
   SELECT person_name, efficiency_grade, COUNT(*)
   FROM players_processed 
   WHERE efficiency_grade IS NOT NULL
   GROUP BY person_name, efficiency_grade
   ORDER BY efficiency_grade, COUNT(*) DESC;
   ```

2. **Analyze Defensive Performance:**
   ```sql
   -- Top defensive players
   SELECT person_name, 
          ROUND(AVG(defensive_impact_score), 1) as avg_def_score,
          defensive_grade
   FROM players_processed 
   WHERE minutes_played >= 15
   GROUP BY person_name, defensive_grade
   ORDER BY avg_def_score DESC
   LIMIT 20;
   ```

3. **Build Analytics Applications:**
   - Your database is now ready for AI agent integration
   - All advanced metrics are calculated and stored
   - Optimized for analytical queries with proper indexing

## 📚 Resources

- **Database Schema:** All tables documented in `src/nba_analyst/database/models.py`
- **Processing Pipeline:** Complete workflow in `src/nba_analyst/processing/`
- **Advanced Metrics:** Basketball analytics in `src/nba_analyst/analytics/`
- **Configuration:** Settings in `src/nba_analyst/config/`

Your NBA Analytics database is now ready! 🏀📊🚀