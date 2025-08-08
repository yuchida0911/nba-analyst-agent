#!/usr/bin/env python3
"""
Debug script to identify the column name mismatch issue.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from nba_analyst.ingestion.csv_reader import NBACSVReader
from nba_analyst.ingestion.ingest import NBADataIngestion
from nba_analyst.database.models import PlayerBoxScore

def main():
    print("🔍 Debugging column name issues...")
    
    # Step 1: Check what the CSV reader produces
    print("\n1. CSV Reader Output:")
    reader = NBACSVReader()
    result = reader.read_csv_file('NBA-Data-2010-2024/play_off_box_scores_2010_2024.csv', max_rows=5)
    
    if result.success:
        print(f"   DataFrame shape: {result.data.shape}")
        print("   DataFrame columns:")
        for col in sorted(result.data.columns):
            print(f"     {col}")
        
        print("\n   First row data:")
        first_row = result.data.iloc[0]
        for col in result.data.columns:
            print(f"     {col}: {first_row[col]}")
    else:
        print("   CSV reading failed:", result.errors)
        return
    
    # Step 2: Check what the ingestion module expects
    print("\n2. PlayerBoxScore Model Attributes:")
    model_attrs = []
    for attr_name in dir(PlayerBoxScore):
        if not attr_name.startswith('_') and hasattr(getattr(PlayerBoxScore, attr_name), 'type'):
            model_attrs.append(attr_name)
    
    for attr in sorted(model_attrs):
        print(f"     {attr}")
    
    # Step 3: Try the row conversion
    print("\n3. Testing Row Conversion:")
    ingestion = NBADataIngestion()
    first_row = result.data.iloc[0]
    
    try:
        converted_data = ingestion._row_to_model_data(first_row, 'box_scores')
        if converted_data:
            print("   Conversion successful!")
            print("   Converted data keys:")
            for key in sorted(converted_data.keys()):
                print(f"     {key}: {converted_data[key]}")
        else:
            print("   Conversion failed - returned None")
    except Exception as e:
        print(f"   Conversion failed with error: {e}")
    
    # Step 4: Check for column name mismatches
    print("\n4. Column Name Analysis:")
    df_columns = set(result.data.columns)
    
    # Expected column names from ingestion mapping
    expected_columns = {
        'gameId', 'personId', 'season_year', 'game_date', 'matchup',
        'teamId', 'teamCity', 'teamName', 'teamTricode', 'teamSlug',
        'personName', 'position', 'comment', 'jerseyNum', 'minutes',
        'fieldGoalsMade', 'fieldGoalsAttempted', 'fieldGoalsPercentage',
        'threePointersMade', 'threePointersAttempted', 'threePointersPercentage',
        'freeThrowsMade', 'freeThrowsAttempted', 'freeThrowsPercentage',
        'reboundsOffensive', 'reboundsDefensive', 'reboundsTotal',
        'assists', 'steals', 'blocks', 'turnovers', 'foulsPersonal',
        'points', 'plusMinusPoints'
    }
    
    print("   Columns in DataFrame but not expected:")
    for col in sorted(df_columns - expected_columns):
        print(f"     {col}")
    
    print("   Expected columns not in DataFrame:")
    for col in sorted(expected_columns - df_columns):
        print(f"     {col}")

if __name__ == "__main__":
    main()