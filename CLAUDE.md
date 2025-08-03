# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an NBA AI Data Analysis System that creates AI-powered analytics for NBA player and team performance. The system transforms raw NBA CSV data into AI-friendly formats and provides intelligent analysis through an agent-based architecture.

## Repository Structure

```
nba-analyst-agent/
├── NBA-Data-2010-2024/        # Git submodule containing NBA CSV datasets (2010-2024)
├── data/                      # Data table specifications
│   ├── box_scores_schema.json # Player-level game statistics schema
│   └── totals_schema.json     # Team-level game statistics schema
└── docs/                      # Project documentation
    ├── PRD.md                 # Product Requirements Document
    └── nba_agent_interaction_examples.md  # AI agent interaction patterns
```

## Data Architecture

### Core Data Sources
- **NBA-Data-2010-2024/**: Git submodule containing 14+ years of NBA data
  - `box_scores` CSV files: Player-level game statistics (~500K+ records)
  - `totals` CSV files: Team-level game statistics with rankings
  - Data covers regular season and playoff games (2010-2024)

### Data Schema Files
Located in `data/` directory:
- **box_scores_schema.json**: Comprehensive schema for player game statistics
  - 35 fields including shooting metrics, rebounds, assists, advanced stats
  - Business rules for data validation and integrity
  - AI-specific usage guidelines
- **totals_schema.json**: Team-level game statistics and rankings
  - 47 fields including team stats and league rankings
  - Cross-references with box scores data

Both schema files are designed for AI agent compatibility with:
- Detailed field descriptions and business context
- Data validation rules and constraints
- Example values and relationship mappings
- AI-specific usage notes for analysis

## System Architecture (Planned)

### Technology Stack
- **Programming Language**: Python 3.9+
- **Database**: PostgreSQL 13+ for analytical queries
- **AI Framework**: Google Agent Development Kit (ADK)
- **LLM Providers**: Google Gemini (gemini-pro), Anthropic Claude (claude-3-sonnet-20240229)
- **Data Processing**: pandas, numpy, sqlalchemy
- **Visualization**: plotly, matplotlib

### Database Design
Planned core tables:
- `players_raw`: Direct CSV import staging
- `players_processed`: AI-ready processed data
- `player_monthly_trends`: Aggregated monthly performance with recency weighting
- `clutch_performances`: High-pressure situation analysis
- `prediction_models`: ML model metadata and results

### AI Agent Architecture
- **Agent Configuration**: Temperature 0.1, ReAct strategy, 4000 token memory buffer
- **Analysis Tools**: 
  - Player Statistics Query Tool
  - Scoring Efficiency Analyzer  
  - Clutch Performance Analyzer
  - Report Generator (HTML format)
- **Analysis Types**: Comprehensive, Scoring, Defense, Clutch performance

## Key Features

### Advanced Analytics
- **Derived Metrics**: True Shooting %, Effective FG%, Usage Rate, PER, Defensive Impact Score
- **Trend Analysis**: Monthly aggregation with exponential decay weighting (default 0.95)
- **Clutch Analysis**: Performance in high-pressure situations (close games, final minutes)
- **Contextual Comparisons**: Position-based, league averages, peer rankings

### Performance Requirements
- Query response time: < 2 seconds for single player analysis
- Report generation: Complete reports in < 30 seconds
- Data processing: 100K+ records in < 10 minutes
- Database support: Up to 10M+ records efficiently

## Development Guidelines

### Data Handling
- All CSV data processing should reference the schema files in `data/` for field definitions
- Implement data validation using the business rules defined in schema files
- Handle missing values and DNP (Did Not Play) cases according to schema specifications
- Use the relationship mappings for joins between box scores and totals data

### AI Agent Development
- Reference `docs/nba_agent_interaction_examples.md` for interaction patterns
- Implement natural language processing for basketball analytics queries
- Support multi-turn conversations with context retention
- Provide detailed analysis with actionable insights and recommendations

### Analysis Focus Areas
- **Efficiency Analysis**: Shooting percentages, true shooting, effective field goal percentage
- **Trend Detection**: Performance trajectories with recency weighting
- **Defensive Impact**: Beyond traditional stats, composite defensive metrics
- **Clutch Performance**: Separate analysis of high-pressure situations
- **Comparative Analysis**: Player rankings, peer comparisons, league context

## Data Processing Pipeline

1. **Ingestion**: Monitor and process CSV files from NBA-Data-2010-2024 submodule
2. **Transformation**: Apply schema definitions and calculate derived metrics
3. **Storage**: Optimize for analytical queries with proper indexing
4. **Analysis**: AI agent tools query processed data for insights
5. **Reporting**: Generate analyst-level reports with visualizations

## Current Status

This is a planning-stage repository. The main components currently available are:
- Complete data schema specifications (AI-compatible JSON format)
- Comprehensive requirements documentation
- AI agent interaction design patterns
- NBA dataset integration via git submodule

Implementation follows the phased approach outlined in `docs/PRD.md`:
- Phase 1: Core Infrastructure (Database, data pipeline)
- Phase 2: Analytics Engine (Advanced metrics, trends)
- Phase 3: AI Agent Integration (Google ADK, custom tools)
- Phase 4: Reporting System (HTML reports, visualizations)
- Phase 5: Testing and Validation

When implementing features, always reference the PRD for detailed requirements and success criteria.