"""
Data Transformation and Derived Metrics Calculation

This module handles the transformation of raw NBA data into AI-ready
formats with calculated derived metrics and business rule validation.
"""

from typing import Dict, Any, Optional, List
from datetime import date, datetime
import logging

from ..analytics.metrics import (
    PlayerGameStats,
    calculate_true_shooting_percentage,
    calculate_effective_field_goal_percentage,
    calculate_usage_rate,
    calculate_player_efficiency_rating
)
from ..analytics.defensive import calculate_defensive_impact_score
from ..analytics.efficiency import EfficiencyAnalyzer
from ..database.models import PlayerBoxScore


class DataTransformer:
    """
    Handles transformation of raw NBA data into processed formats.
    
    Applies business rules, calculates derived metrics, and prepares
    data for AI analysis according to the schema specifications.
    """
    
    def __init__(self):
        """Initialize the data transformer."""
        self.logger = logging.getLogger(__name__)
        self.efficiency_analyzer = EfficiencyAnalyzer()
    
    def transform_player_game(self, raw_player: PlayerBoxScore) -> Dict[str, Any]:
        """
        Transform a single player's game data with derived metrics.
        
        Args:
            raw_player: Raw player box score data
            
        Returns:
            Dictionary with transformed data and calculated metrics
        """
        try:
            # Convert raw data to analytics format
            stats = self._convert_to_game_stats(raw_player)
            
            # Calculate advanced metrics
            advanced_metrics = self._calculate_advanced_metrics(stats)
            
            # Apply business rules and validation
            validation_result = self._apply_business_rules(raw_player, stats)
            
            # Calculate per-36 minute statistics
            per_36_stats = self._calculate_per_36_stats(stats)
            
            # Determine performance grades
            grades = self._calculate_performance_grades(advanced_metrics)
            
            # Create transformed data dictionary
            transformed_data = {
                # Basic game information
                'game_id': raw_player.game_id,
                'person_id': raw_player.person_id,
                'season_year': raw_player.season_year,
                'game_date': raw_player.game_date,
                'matchup': raw_player.matchup,
                'person_name': raw_player.person_name,
                'team_id': raw_player.team_id,
                'team_name': raw_player.team_name,
                'team_tricode': raw_player.team_tricode,
                'position': raw_player.position,
                
                # Playing time (converted to decimal)
                'minutes_played': stats.minutes_played,
                'is_dnp': raw_player.is_dnp,
                
                # Basic statistics
                'points': stats.points,
                'field_goals_made': stats.field_goals_made,
                'field_goals_attempted': stats.field_goals_attempted,
                'three_pointers_made': stats.three_pointers_made,
                'three_pointers_attempted': stats.three_pointers_attempted,
                'free_throws_made': stats.free_throws_made,
                'free_throws_attempted': stats.free_throws_attempted,
                'rebounds_offensive': stats.rebounds_offensive,
                'rebounds_defensive': stats.rebounds_defensive,
                'rebounds_total': stats.rebounds_total,
                'assists': stats.assists,
                'steals': stats.steals,
                'blocks': stats.blocks,
                'turnovers': stats.turnovers,
                'fouls_personal': stats.fouls_personal,
                'plus_minus': raw_player.plus_minus_points or 0,
                
                # Advanced metrics
                **advanced_metrics,
                
                # Per-36 statistics
                **per_36_stats,
                
                # Performance grades
                **grades,
                
                # Metadata
                'processed_at': date.today(),
                'source_validation_passed': validation_result['passed'],
                'validation_warnings': validation_result['warnings']
            }
            
            return transformed_data
            
        except Exception as e:
            self.logger.error(f"Error transforming player {raw_player.person_name}: {str(e)}")
            raise
    
    def _convert_to_game_stats(self, raw_player: PlayerBoxScore) -> PlayerGameStats:
        """Convert raw player data to PlayerGameStats format."""
        
        # Convert minutes to decimal format
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
    
    def _calculate_advanced_metrics(self, stats: PlayerGameStats) -> Dict[str, Optional[float]]:
        """Calculate advanced basketball metrics."""
        
        # Basic shooting percentages
        field_goal_pct = None
        if stats.field_goals_attempted > 0:
            field_goal_pct = stats.field_goals_made / stats.field_goals_attempted
        
        three_point_pct = None
        if stats.three_pointers_attempted > 0:
            three_point_pct = stats.three_pointers_made / stats.three_pointers_attempted
        
        free_throw_pct = None
        if stats.free_throws_attempted > 0:
            free_throw_pct = stats.free_throws_made / stats.free_throws_attempted
        
        # Advanced metrics
        return {
            'field_goal_percentage': field_goal_pct,
            'three_point_percentage': three_point_pct,
            'free_throw_percentage': free_throw_pct,
            'true_shooting_percentage': calculate_true_shooting_percentage(stats),
            'effective_field_goal_percentage': calculate_effective_field_goal_percentage(stats),
            'usage_rate': calculate_usage_rate(stats),
            'player_efficiency_rating': calculate_player_efficiency_rating(stats),
            'defensive_impact_score': calculate_defensive_impact_score(stats)
        }
    
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
    
    def _calculate_performance_grades(self, metrics: Dict[str, Optional[float]]) -> Dict[str, Optional[str]]:
        """Calculate performance grades for efficiency and defense."""
        
        efficiency_grade = None
        if metrics.get('true_shooting_percentage') is not None:
            efficiency_grade = self.efficiency_analyzer.grade_efficiency(
                metrics['true_shooting_percentage']
            )
        
        defensive_grade = None
        if metrics.get('defensive_impact_score') is not None:
            from ..analytics.defensive import grade_defensive_performance
            defensive_grade = grade_defensive_performance(metrics['defensive_impact_score'])
        
        return {
            'efficiency_grade': efficiency_grade,
            'defensive_grade': defensive_grade
        }
    
    def _apply_business_rules(self, raw_player: PlayerBoxScore, stats: PlayerGameStats) -> Dict[str, Any]:
        """Apply business rules validation according to schema specifications."""
        
        warnings = []
        passed = True
        
        try:
            # Use the model's built-in validation
            model_errors = raw_player.validate_data_integrity()
            if model_errors:
                warnings.extend(model_errors)
            
            # Additional business rules from schema
            
            # Check for reasonable statistical ranges
            if stats.points > 100:
                warnings.append(f"Unusually high points: {stats.points}")
            
            if stats.minutes_played > 60:
                warnings.append(f"Unusually high minutes: {stats.minutes_played}")
            
            # Check shooting consistency
            if (stats.field_goals_attempted > 0 and 
                stats.field_goals_made / stats.field_goals_attempted > 1.0):
                warnings.append("Field goal percentage exceeds 100%")
                passed = False
            
            # Check for DNP consistency
            if raw_player.is_dnp and stats.minutes_played > 0:
                warnings.append("Player marked as DNP but has minutes played")
            
            # Validate season year format
            if not self._is_valid_season_format(raw_player.season_year):
                warnings.append(f"Invalid season year format: {raw_player.season_year}")
            
        except Exception as e:
            warnings.append(f"Validation error: {str(e)}")
            passed = False
        
        return {
            'passed': passed,
            'warnings': warnings
        }
    
    def _is_valid_season_format(self, season_year: str) -> bool:
        """Validate season year format (e.g., '2023-24')."""
        if not season_year or len(season_year) != 7:
            return False
        
        parts = season_year.split('-')
        if len(parts) != 2:
            return False
        
        try:
            year1 = int(parts[0])
            year2 = int(parts[1])
            
            # Check that second year is first year + 1
            if year2 != (year1 + 1) % 100:
                return False
            
            # Check reasonable year range
            if year1 < 1946 or year1 > 2030:  # NBA founded in 1946
                return False
            
            return True
        except ValueError:
            return False


class AdvancedMetricsCalculator:
    """
    Specialized calculator for advanced NBA metrics.
    
    Handles complex calculations that require multiple data points
    or contextual information beyond single game statistics.
    """
    
    def __init__(self):
        """Initialize the advanced metrics calculator."""
        self.logger = logging.getLogger(__name__)
    
    def calculate_contextual_metrics(
        self, 
        player_stats: PlayerGameStats,
        team_context: Optional[Dict[str, Any]] = None,
        league_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Optional[float]]:
        """
        Calculate advanced metrics that require contextual information.
        
        Args:
            player_stats: Individual player statistics
            team_context: Team-level context data (optional)
            league_context: League-wide context data (optional)
            
        Returns:
            Dictionary with contextual advanced metrics
        """
        contextual_metrics = {}
        
        # Enhanced Usage Rate with team context
        if team_context:
            enhanced_usage = self._calculate_enhanced_usage_rate(
                player_stats, team_context
            )
            contextual_metrics['enhanced_usage_rate'] = enhanced_usage
        
        # League-relative metrics
        if league_context:
            league_relative = self._calculate_league_relative_metrics(
                player_stats, league_context
            )
            contextual_metrics.update(league_relative)
        
        return contextual_metrics
    
    def _calculate_enhanced_usage_rate(
        self, 
        player_stats: PlayerGameStats,
        team_context: Dict[str, Any]
    ) -> Optional[float]:
        """Calculate usage rate with actual team possession data."""
        
        if player_stats.minutes_played <= 0:
            return None
        
        # Get team possessions from context
        team_possessions = team_context.get('team_possessions')
        if not team_possessions:
            return None
        
        # Player possessions used
        player_possessions = (
            player_stats.field_goals_attempted + 
            (0.44 * player_stats.free_throws_attempted) +
            player_stats.turnovers
        )
        
        # Calculate possession-based usage rate
        return min(player_possessions / team_possessions, 1.0)
    
    def _calculate_league_relative_metrics(
        self,
        player_stats: PlayerGameStats,
        league_context: Dict[str, Any]
    ) -> Dict[str, Optional[float]]:
        """Calculate metrics relative to league averages."""
        
        league_metrics = {}
        
        # True Shooting relative to league average
        ts_pct = calculate_true_shooting_percentage(player_stats)
        league_avg_ts = league_context.get('avg_true_shooting', 0.56)
        
        if ts_pct is not None:
            league_metrics['ts_relative_to_league'] = ts_pct - league_avg_ts
        
        # PER relative to league average (15.0 is defined league average)
        per = calculate_player_efficiency_rating(player_stats)
        if per is not None:
            league_metrics['per_relative_to_league'] = per - 15.0
        
        return league_metrics