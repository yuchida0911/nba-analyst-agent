"""
Data Transformation Pipeline for Advanced Analytics

This module transforms raw NBA player data into AI-optimized processed data
with calculated advanced metrics, ready for analysis and reporting.
"""

from typing import List, Optional, Dict, Any
from datetime import date, datetime
from dataclasses import dataclass

from ..database.models import PlayerBoxScore, PlayerProcessed, PlayerMonthlyTrend
from ..database.connection import DatabaseConnection
from .metrics import (
    PlayerGameStats, 
    calculate_true_shooting_percentage,
    calculate_effective_field_goal_percentage,
    calculate_usage_rate,
    calculate_player_efficiency_rating,
    calculate_advanced_metrics_summary
)
from .defensive import (
    calculate_defensive_impact_score,
    grade_defensive_performance
)
from .efficiency import EfficiencyAnalyzer


@dataclass
class ProcessingResult:
    """Result of data processing operation."""
    
    success: bool
    processed_count: int
    skipped_count: int
    error_count: int
    errors: List[str]
    
    @property
    def total_records(self) -> int:
        """Total records processed (success + skip + error)."""
        return self.processed_count + self.skipped_count + self.error_count


class AdvancedMetricsProcessor:
    """
    Processor for transforming raw player data into advanced analytics.
    
    Takes raw player box scores and calculates advanced metrics,
    grades, and AI-ready features for analysis.
    """
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize the advanced metrics processor.
        
        Args:
            db_connection: Database connection for data operations
        """
        self.db_connection = db_connection
        self.efficiency_analyzer = EfficiencyAnalyzer()
    
    def _convert_to_player_game_stats(self, raw_player: PlayerBoxScore) -> PlayerGameStats:
        """Convert raw player data to PlayerGameStats for metrics calculation."""
        
        # Convert minutes from MM:SS to decimal if needed
        minutes_decimal = raw_player.minutes_decimal or 0.0
        
        return PlayerGameStats(
            points=raw_player.points or 0,
            field_goals_made=raw_player.field_goals_made or 0,
            field_goals_attempted=raw_player.field_goals_attempted or 0,
            three_pointers_made=raw_player.three_pointers_made or 0,
            three_pointers_attempted=raw_player.three_pointers_attempted or 0,
            free_throws_made=raw_player.free_throws_made or 0,
            free_throws_attempted=raw_player.free_throws_attempted or 0,
            rebounds_offensive=raw_player.rebounds_offensive or 0,
            rebounds_defensive=raw_player.rebounds_defensive or 0,
            rebounds_total=raw_player.rebounds_total or 0,
            assists=raw_player.assists or 0,
            steals=raw_player.steals or 0,
            blocks=raw_player.blocks or 0,
            turnovers=raw_player.turnovers or 0,
            fouls_personal=raw_player.fouls_personal or 0,
            minutes_played=minutes_decimal
        )
    
    def _calculate_per_36_stats(self, stats: PlayerGameStats) -> Dict[str, Optional[float]]:
        """Calculate per-36 minute statistics."""
        if stats.minutes_played <= 0:
            return {
                'points_per_36': None,
                'rebounds_per_36': None,
                'assists_per_36': None,
                'steals_per_36': None,
                'blocks_per_36': None
            }
        
        multiplier = 36.0 / stats.minutes_played
        
        return {
            'points_per_36': stats.points * multiplier,
            'rebounds_per_36': stats.rebounds_total * multiplier,
            'assists_per_36': stats.assists * multiplier,
            'steals_per_36': stats.steals * multiplier,
            'blocks_per_36': stats.blocks * multiplier
        }
    
    def _calculate_basic_percentages(self, stats: PlayerGameStats) -> Dict[str, Optional[float]]:
        """Calculate basic shooting percentages."""
        percentages = {}
        
        # Field goal percentage  
        if stats.field_goals_attempted > 0:
            percentages['field_goal_pct'] = stats.field_goals_made / stats.field_goals_attempted
        else:
            percentages['field_goal_pct'] = None
        
        # Three point percentage
        if stats.three_pointers_attempted > 0:
            percentages['three_point_pct'] = stats.three_pointers_made / stats.three_pointers_attempted
        else:
            percentages['three_point_pct'] = None
        
        # Free throw percentage
        if stats.free_throws_attempted > 0:
            percentages['free_throw_pct'] = stats.free_throws_made / stats.free_throws_attempted
        else:
            percentages['free_throw_pct'] = None
        
        return percentages
    
    def process_player_game(self, raw_player: PlayerBoxScore) -> Optional[PlayerProcessed]:
        """
        Process a single player's game data into advanced metrics.
        
        Args:
            raw_player: Raw player box score data
            
        Returns:
            PlayerProcessed object with calculated metrics, None if processing fails
        """
        try:
            # Convert to analytics format
            stats = self._convert_to_player_game_stats(raw_player)
            
            # Calculate advanced metrics
            ts_pct = calculate_true_shooting_percentage(stats)
            efg_pct = calculate_effective_field_goal_percentage(stats)
            usage_rate = calculate_usage_rate(stats)
            per = calculate_player_efficiency_rating(stats)
            defensive_impact = calculate_defensive_impact_score(stats)
            
            # Calculate basic percentages
            basic_pcts = self._calculate_basic_percentages(stats)
            
            # Calculate per-36 stats
            per_36_stats = self._calculate_per_36_stats(stats)
            
            # Grade performance
            from .efficiency import EfficiencyAnalyzer
            efficiency_grade = None
            if ts_pct is not None:
                analyzer = EfficiencyAnalyzer()
                efficiency_grade = analyzer.grade_efficiency(ts_pct)
            
            defensive_grade = None
            if defensive_impact is not None:
                defensive_grade = grade_defensive_performance(defensive_impact)
            
            # Create processed player record
            processed_player = PlayerProcessed(
                game_id=raw_player.game_id,
                person_id=raw_player.person_id,
                season_year=raw_player.season_year,
                game_date=raw_player.game_date,
                matchup=raw_player.matchup,
                person_name=raw_player.person_name,
                team_id=raw_player.team_id,
                team_name=raw_player.team_name,
                team_tricode=raw_player.team_tricode,
                position=raw_player.position,
                minutes_played=stats.minutes_played,
                is_dnp=raw_player.is_dnp,
                
                # Basic stats
                points=stats.points,
                field_goals_made=stats.field_goals_made,
                field_goals_attempted=stats.field_goals_attempted,
                three_pointers_made=stats.three_pointers_made,
                three_pointers_attempted=stats.three_pointers_attempted,
                free_throws_made=stats.free_throws_made,
                free_throws_attempted=stats.free_throws_attempted,
                rebounds_offensive=stats.rebounds_offensive,
                rebounds_defensive=stats.rebounds_defensive,
                rebounds_total=stats.rebounds_total,
                assists=stats.assists,
                steals=stats.steals,
                blocks=stats.blocks,
                turnovers=stats.turnovers,
                fouls_personal=stats.fouls_personal,
                plus_minus=raw_player.plus_minus_points or 0,
                
                # Advanced shooting metrics
                true_shooting_percentage=ts_pct,
                effective_field_goal_percentage=efg_pct,
                field_goal_percentage=basic_pcts['field_goal_pct'],
                three_point_percentage=basic_pcts['three_point_pct'],
                free_throw_percentage=basic_pcts['free_throw_pct'],
                
                # Advanced performance metrics
                player_efficiency_rating=per,
                usage_rate=usage_rate,
                defensive_impact_score=defensive_impact,
                
                # Per-36 stats
                points_per_36=per_36_stats['points_per_36'],
                rebounds_per_36=per_36_stats['rebounds_per_36'],
                assists_per_36=per_36_stats['assists_per_36'],
                steals_per_36=per_36_stats['steals_per_36'],
                blocks_per_36=per_36_stats['blocks_per_36'],
                
                # Performance grades
                efficiency_grade=efficiency_grade,
                defensive_grade=defensive_grade,
                
                # Metadata
                processed_at=date.today(),
                source_validation_passed=True  # Assume raw data is validated
            )
            
            return processed_player
            
        except Exception as e:
            # Log error but don't crash processing
            print(f"Error processing player {raw_player.person_name} (ID: {raw_player.person_id}): {str(e)}")
            return None
    
    def process_season_data(self, season_year: str, batch_size: int = 1000) -> ProcessingResult:
        """
        Process all raw player data for a season into advanced metrics.
        
        Args:
            season_year: Season to process (e.g., '2023-24')
            batch_size: Number of records to process in each batch
            
        Returns:
            ProcessingResult with operation statistics
        """
        processed_count = 0
        skipped_count = 0
        error_count = 0
        errors = []
        
        try:
            with self.db_connection.get_session() as session:
                # Query raw data for the season
                query = session.query(PlayerBoxScore).filter(
                    PlayerBoxScore.season_year == season_year
                ).order_by(PlayerBoxScore.game_date, PlayerBoxScore.person_id)
                
                # Process in batches
                offset = 0
                while True:
                    batch = query.offset(offset).limit(batch_size).all()
                    
                    if not batch:
                        break  # No more data
                    
                    batch_processed = []
                    
                    for raw_player in batch:
                        # Check if already processed
                        existing = session.query(PlayerProcessed).filter(
                            PlayerProcessed.game_id == raw_player.game_id,
                            PlayerProcessed.person_id == raw_player.person_id
                        ).first()
                        
                        if existing:
                            skipped_count += 1
                            continue
                        
                        # Process the player
                        processed_player = self.process_player_game(raw_player)
                        
                        if processed_player:
                            batch_processed.append(processed_player)
                            processed_count += 1
                        else:
                            error_count += 1
                            errors.append(f"Failed to process {raw_player.person_name} game {raw_player.game_id}")
                    
                    # Bulk insert processed data
                    if batch_processed:
                        session.bulk_save_objects(batch_processed)
                        session.commit()
                    
                    offset += batch_size
            
            return ProcessingResult(
                success=True,
                processed_count=processed_count,
                skipped_count=skipped_count,
                error_count=error_count,
                errors=errors
            )
            
        except Exception as e:
            return ProcessingResult(
                success=False,
                processed_count=processed_count,
                skipped_count=skipped_count,
                error_count=error_count,
                errors=errors + [f"Processing failed: {str(e)}"]
            )
    
    def get_processing_summary(self, results: List[ProcessingResult]) -> Dict[str, Any]:
        """
        Generate summary statistics from multiple processing results.
        
        Args:
            results: List of ProcessingResult objects
            
        Returns:
            Dictionary with summary statistics
        """
        if not results:
            return {'error': 'No results provided'}
        
        total_processed = sum(r.processed_count for r in results)
        total_skipped = sum(r.skipped_count for r in results)
        total_errors = sum(r.error_count for r in results)
        total_records = sum(r.total_records for r in results)
        
        successful_operations = sum(1 for r in results if r.success)
        all_errors = []
        for r in results:
            all_errors.extend(r.errors)
        
        return {
            'operations_run': len(results),
            'successful_operations': successful_operations,
            'success_rate': successful_operations / len(results) if results else 0.0,
            'total_records_processed': total_processed,
            'total_records_skipped': total_skipped,
            'total_errors': total_errors,
            'total_records': total_records,
            'processing_efficiency': total_processed / total_records if total_records > 0 else 0.0,
            'error_details': all_errors[:10]  # Limit to first 10 errors
        }


def create_advanced_metrics_processor(db_connection: DatabaseConnection) -> AdvancedMetricsProcessor:
    """
    Factory function to create an AdvancedMetricsProcessor.
    
    Args:
        db_connection: Database connection
        
    Returns:
        Configured AdvancedMetricsProcessor instance
    """
    return AdvancedMetricsProcessor(db_connection)