#!/usr/bin/env python3
"""
Clean NBA totals CSV file to fix data quality issues.

This script addresses the following issues in the totals CSV file:
1. Line breaks in the middle of records
2. Missing AVAILABLE_FLAG values
3. Malformed CSV structure

Usage:
    python scripts/clean_totals_csv.py
    python scripts/clean_totals_csv.py --input-file NBA-Data-2010-2024/regular_season_totals_2010_2024.csv
    python scripts/clean_totals_csv.py --output-file cleaned_totals.csv
"""
import sys
import os
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)

def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def read_csv_chunk(file_path: str, chunk_size: int = 1000) -> List[str]:
    """
    Read CSV file in chunks to handle large files efficiently.
    
    Args:
        file_path: Path to the CSV file
        chunk_size: Number of lines to read at once
        
    Returns:
        List of lines from the file
    """
    lines = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                lines.append(line.strip())
                if len(lines) >= chunk_size:
                    yield lines
                    lines = []
            
            # Yield remaining lines
            if lines:
                yield lines
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        raise

def count_columns(line: str) -> int:
    """Count the number of columns in a CSV line."""
    return line.count(',') + 1

def is_complete_record(line: str, expected_columns: int = 57) -> bool:
    """
    Check if a line represents a complete CSV record.
    
    Args:
        line: CSV line to check
        expected_columns: Expected number of columns (including AVAILABLE_FLAG)
        
    Returns:
        True if the line has the expected number of columns
    """
    return count_columns(line) == expected_columns

def fix_line_breaks(lines: List[str]) -> List[str]:
    """
    Fix line breaks in CSV data by joining incomplete records.
    
    Args:
        lines: List of CSV lines
        
    Returns:
        List of fixed CSV lines
    """
    fixed_lines = []
    current_record = ""
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
            
        # If this line has the expected number of columns, it's a complete record
        if is_complete_record(line):
            if current_record:
                # Join with the current record if we have one
                combined = current_record + line
                if is_complete_record(combined):
                    fixed_lines.append(combined)
                else:
                    # If still incomplete, continue building
                    current_record = combined
            else:
                # Complete record on its own
                fixed_lines.append(line)
            current_record = ""
        else:
            # Incomplete record, add to current
            if current_record:
                current_record += " " + line
            else:
                current_record = line
    
    # Handle any remaining incomplete record
    if current_record:
        logger.warning(f"Found incomplete record at end: {current_record[:100]}...")
        # Try to complete it with default AVAILABLE_FLAG
        if not current_record.endswith(',1.0'):
            current_record += ',1.0'
        if is_complete_record(current_record):
            fixed_lines.append(current_record)
    
    return fixed_lines

def add_missing_available_flag(lines: List[str]) -> List[str]:
    """
    Add missing AVAILABLE_FLAG values to CSV lines.
    
    Args:
        lines: List of CSV lines
        
    Returns:
        List of CSV lines with AVAILABLE_FLAG added where missing
    """
    fixed_lines = []
    
    for line in lines:
        if not line.strip():
            continue
            
        # Remove trailing comma if present
        cleaned_line = line.rstrip(',')
        
        # Check if line ends with AVAILABLE_FLAG
        if cleaned_line.endswith(',1.0') or cleaned_line.endswith(',1'):
            # Already has AVAILABLE_FLAG
            fixed_lines.append(cleaned_line)
        elif cleaned_line.endswith(',0.0') or cleaned_line.endswith(',0'):
            # Has AVAILABLE_FLAG with 0
            fixed_lines.append(cleaned_line)
        else:
            # Missing AVAILABLE_FLAG, add it
            fixed_line = cleaned_line + ',1.0'
            fixed_lines.append(fixed_line)
    
    return fixed_lines

def validate_csv_structure(lines: List[str]) -> Tuple[int, int, List[str]]:
    """
    Validate CSV structure and count issues.
    
    Args:
        lines: List of CSV lines
        
    Returns:
        Tuple of (total_lines, valid_lines, issues)
    """
    total_lines = len(lines)
    valid_lines = 0
    issues = []
    
    for i, line in enumerate(lines, 1):
        if not line.strip():
            continue
            
        column_count = count_columns(line)
        if column_count == 57:
            valid_lines += 1
        else:
            issues.append(f"Line {i}: Expected 57 columns, found {column_count}")
    
    return total_lines, valid_lines, issues

def clean_totals_csv(input_file: str, output_file: str) -> bool:
    """
    Clean the totals CSV file to fix data quality issues.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output cleaned CSV file
        
    Returns:
        True if cleaning was successful
    """
    start_time = datetime.now()
    
    try:
        logger.info(f"🧹 Starting CSV cleaning process")
        logger.info(f"Input file: {input_file}")
        logger.info(f"Output file: {output_file}")
        
        # Read and process the file in chunks
        all_fixed_lines = []
        chunk_count = 0
        
        for chunk in read_csv_chunk(input_file, chunk_size=5000):
            chunk_count += 1
            logger.info(f"Processing chunk {chunk_count} ({len(chunk)} lines)")
            
            # Fix line breaks in this chunk
            fixed_chunk = fix_line_breaks(chunk)
            logger.debug(f"Fixed line breaks: {len(chunk)} -> {len(fixed_chunk)} lines")
            
            # Add missing AVAILABLE_FLAG values
            fixed_chunk = add_missing_available_flag(fixed_chunk)
            logger.debug(f"Added missing AVAILABLE_FLAG values")
            
            all_fixed_lines.extend(fixed_chunk)
        
        # Validate the cleaned data
        total_lines, valid_lines, issues = validate_csv_structure(all_fixed_lines)
        
        logger.info(f"📊 Validation results:")
        logger.info(f"   • Total lines: {total_lines}")
        logger.info(f"   • Valid lines: {valid_lines}")
        logger.info(f"   • Issues found: {len(issues)}")
        
        if issues:
            logger.warning("⚠️  Found data quality issues:")
            for issue in issues[:10]:  # Show first 10 issues
                logger.warning(f"   • {issue}")
            if len(issues) > 10:
                logger.warning(f"   • ... and {len(issues) - 10} more issues")
        
        # Write the cleaned data
        logger.info(f"💾 Writing cleaned data to {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            for line in all_fixed_lines:
                f.write(line + '\n')
        
        # Final validation
        duration = (datetime.now() - start_time).total_seconds()
        success_rate = (valid_lines / total_lines) * 100 if total_lines > 0 else 0
        
        logger.info(f"✅ CSV cleaning completed in {duration:.2f}s")
        logger.info(f"📈 Success rate: {success_rate:.1f}%")
        logger.info(f"📁 Output file: {output_file}")
        logger.info(f"📏 File size: {os.path.getsize(output_file):,} bytes")
        
        return success_rate >= 95.0  # Consider successful if 95%+ valid
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"❌ Failed to clean CSV after {duration:.2f}s: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        return False

def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Clean NBA totals CSV file to fix data quality issues",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Clean with default settings
    python scripts/clean_totals_csv.py
    
    # Clean specific input file
    python scripts/clean_totals_csv.py --input-file my_data.csv
    
    # Specify output file
    python scripts/clean_totals_csv.py --output-file cleaned_data.csv
    
    # Debug logging
    python scripts/clean_totals_csv.py --log-level DEBUG
        """
    )
    
    parser.add_argument(
        "--input-file",
        default="NBA-Data-2010-2024/regular_season_totals_2010_2024.csv",
        help="Input CSV file to clean (default: NBA-Data-2010-2024/regular_season_totals_2010_2024.csv)"
    )
    
    parser.add_argument(
        "--output-file",
        default="NBA-Data-2010-2024/regular_season_totals_2010_2024_cleaned.csv",
        help="Output cleaned CSV file (default: NBA-Data-2010-2024/regular_season_totals_2010_2024_cleaned.csv)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    logger.info("🚀 NBA Totals CSV Cleaner")
    logger.info("=" * 40)
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Output file: {args.output_file}")
    logger.info(f"Log level: {args.log_level}")
    
    # Check if input file exists
    if not os.path.exists(args.input_file):
        logger.error(f"❌ Input file not found: {args.input_file}")
        sys.exit(1)
    
    # Execute cleaning
    try:
        success = clean_totals_csv(args.input_file, args.output_file)
        
        if success:
            logger.info("✅ CSV cleaning completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ CSV cleaning completed with issues!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("🛑 Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        sys.exit(1)

if __name__ == "__main__":
    main() 