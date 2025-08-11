# NBA Analytics Agent

AI-powered NBA data analysis system with automated data processing and intelligent reporting.

## Features

- Advanced NBA analytics and statistics
- BigQuery integration for large-scale data processing
- AI agent for intelligent query processing
- Comprehensive player and team analysis tools

## Installation

```bash
poetry install
```

## Usage

Run the agent locally:

```bash
poetry run adk run analyst_agent
```

Deploy to Google Cloud Vertex AI Agent Engine:

```bash
cd deployment
poetry run python deploy.py --create
```
