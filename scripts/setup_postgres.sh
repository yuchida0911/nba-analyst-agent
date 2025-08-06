#!/bin/bash
# PostgreSQL Setup Script for NBA Analyst Agent

echo "🏀 NBA Analyst Agent - PostgreSQL Database Setup"
echo "================================================="

# Check if PostgreSQL is installed
if ! command -v psql &> /dev/null; then
    echo "❌ PostgreSQL is not installed!"
    echo "📥 Installing PostgreSQL with Homebrew..."
    
    if ! command -v brew &> /dev/null; then
        echo "❌ Homebrew is not installed. Please install it first:"
        echo "   /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
        exit 1
    fi
    
    brew install postgresql@15
    brew services start postgresql@15
else
    echo "✅ PostgreSQL is already installed"
fi

# Check if PostgreSQL is running
if ! brew services list | grep postgresql | grep started &> /dev/null; then
    echo "🚀 Starting PostgreSQL service..."
    brew services start postgresql@15
fi

echo "🔧 Setting up NBA Analyst database..."

# Create database and user
psql postgres << EOF
-- Create database
DROP DATABASE IF EXISTS nba_analyst;
CREATE DATABASE nba_analyst;

-- Create user
DROP USER IF EXISTS nba_user;
CREATE USER nba_user WITH PASSWORD 'nba_secure_2024';

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE nba_analyst TO nba_user;

-- Connect to the new database and grant schema permissions
\c nba_analyst

GRANT ALL ON SCHEMA public TO nba_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO nba_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO nba_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO nba_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO nba_user;

\q
EOF

echo "✅ Database and user created successfully!"
echo "📋 Database Details:"
echo "   Database: nba_analyst"
echo "   User: nba_user"
echo "   Password: nba_secure_2024"
echo "   Host: localhost"
echo "   Port: 5432"

echo ""
echo "🎯 Next: Copy .env.example to .env and update with these credentials"
echo "   cp .env.example .env"
echo ""
echo "Then run: python scripts/setup_postgres.py"