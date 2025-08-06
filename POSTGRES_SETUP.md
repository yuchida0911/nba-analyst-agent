# PostgreSQL Setup Guide for NBA Analyst Agent

This guide will help you set up PostgreSQL and load your NBA datasets into the database with complete advanced analytics processing.

## 🚀 Quick Start

### 1. Install PostgreSQL

**On macOS (using Homebrew):**
```bash
brew install postgresql@15
brew services start postgresql@15
```

**On Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
```

### 2. Create Database and User

Connect to PostgreSQL and create the database:
```bash
psql postgres
```

Run these SQL commands:
```sql
CREATE DATABASE nba_analyst;
CREATE USER nba_user WITH PASSWORD 'nba_secure_2024';
GRANT ALL PRIVILEGES ON DATABASE nba_analyst TO nba_user;

-- Connect to the new database
\c nba_analyst

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO nba_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO nba_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO nba_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO nba_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO nba_user;

\q
```

### 3. Configure Environment

Copy the example environment file and update with your credentials:
```bash
cp .env.example .env
```

Edit `.env` file:
```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nba_analyst
DB_USER=nba_user
DB_PASSWORD=nba_secure_2024
DB_SCHEMA=public

# Keep other settings as default
BATCH_SIZE=1000
LOG_LEVEL=INFO
DEBUG=false
TESTING=false
```

### 4. Setup Database Schema

Create all required tables and indexes:
```bash
python scripts/setup_postgres.py
```

This will create:
- ✅ `players_raw` - Raw player box score data
- ✅ `teams_raw` - Raw team game totals 
- ✅ `players_processed` - AI-ready processed data with advanced metrics
- ✅ `player_monthly_trends` - Monthly aggregated trends

### 5. Load NBA Data

Load your complete NBA dataset with advanced processing:
```bash
python scripts/load_nba_data.py
```

**Options:**
```bash
# Use custom batch size
python scripts/load_nba_data.py --batch-size 500

# Use different data directory
python scripts/load_nba_data.py --data-dir /path/to/your/nba/data

# Enable debug logging
python scripts/load_nba_data.py --log-level DEBUG
```

## 📊 What Gets Processed

The complete processing pipeline will:

### **Phase 1: Raw Data Ingestion**
- Discovers all CSV files in NBA-Data-2010-2024/
- Categorizes as `box_scores` (player stats) or `totals` (team stats)
- Validates data integrity and applies business rules
- Loads into `players_raw` and `teams_raw` tables

### **Phase 2: Advanced Analytics Processing**
- Calculates advanced basketball metrics:
  - True Shooting Percentage (TS%)
  - Effective Field Goal Percentage (eFG%)  
  - Usage Rate estimation
  - Player Efficiency Rating (PER)
  - Defensive Impact Score (0-100)
- Generates per-36 minute statistics
- Assigns performance grades (A+ to D-)
- Stores in `players_processed` table

### **Phase 3: Trend Analysis** 
- Creates monthly performance aggregations
- Applies recency weighting (0.95 decay factor)
- Calculates trend directions (improving/declining/stable)
- Stores in `player_monthly_trends` table

## 📈 Expected Results

After processing your NBA-Data-2010-2024 datasets, you should have:

- **~500K+ player game records** with advanced metrics
- **~50K+ team game records** 
- **Monthly trend data** for all players across all seasons
- **Complete AI-ready dataset** for analysis and reporting

## 🔍 Verify Your Data

Check that data was loaded successfully:

```sql
-- Connect to database
psql -h localhost -U nba_user -d nba_analyst

-- Check record counts
SELECT 'players_raw' as table_name, COUNT(*) as records FROM players_raw
UNION ALL
SELECT 'teams_raw', COUNT(*) FROM teams_raw  
UNION ALL
SELECT 'players_processed', COUNT(*) FROM players_processed
UNION ALL 
SELECT 'player_monthly_trends', COUNT(*) FROM player_monthly_trends;

-- Check a sample of processed data with advanced metrics
SELECT 
    person_name,
    season_year,
    points,
    true_shooting_pct,
    player_efficiency_rating,
    defensive_impact_score,
    efficiency_grade,
    defensive_grade
FROM players_processed 
WHERE person_name = 'LeBron James'
    AND season_year = '2023-24'
LIMIT 5;
```

## 🚨 Troubleshooting

### Database Connection Issues
```bash
# Check if PostgreSQL is running
brew services list | grep postgresql

# Start PostgreSQL if not running
brew services start postgresql@15

# Test connection manually
psql -h localhost -U nba_user -d nba_analyst
```

### Permission Issues
```bash
# Show database creation commands
python scripts/setup_postgres.py --create-user
```

### Data Loading Issues  
```bash
# Reset database and start fresh
python scripts/setup_postgres.py --reset

# Load data with debug logging
python scripts/load_nba_data.py --log-level DEBUG
```

### Check Logs
```bash
# View processing logs
tail -f logs/nba_data_loading.log
```

## 🎯 Next Steps

Once your data is loaded:

1. **Run Analytics Examples:**
   ```bash
   python examples/analyze_player.py 'LeBron James'
   python examples/team_efficiency.py 'Lakers'
   ```

2. **Query Advanced Metrics:**
   ```sql
   -- Top players by True Shooting %
   SELECT person_name, AVG(true_shooting_pct) as avg_ts
   FROM players_processed 
   WHERE minutes_played >= 20
   GROUP BY person_name
   ORDER BY avg_ts DESC
   LIMIT 10;
   ```

3. **Explore Trend Data:**
   ```sql
   -- Player performance trends
   SELECT * FROM player_monthly_trends 
   WHERE person_name = 'Stephen Curry'
   ORDER BY season_year DESC, month_year DESC;
   ```

Your PostgreSQL database is now ready for advanced NBA analytics! 🏀📊