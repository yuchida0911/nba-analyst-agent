"""
Monthly Trend Analysis and Recency Weighting

This module provides advanced trend analysis for NBA player performance,
including monthly aggregations, recency weighting, and statistical
trend detection using regression analysis.
"""

from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass
from datetime import date, datetime
from collections import defaultdict
import statistics
from .metrics import PlayerGameStats


@dataclass
class MonthlyPerformance:
    """Monthly aggregated performance data."""
    
    year: int
    month: int
    games_played: int
    
    # Averages
    avg_points: float
    avg_rebounds: float
    avg_assists: float
    avg_steals: float
    avg_blocks: float
    avg_turnovers: float
    avg_minutes: float
    
    # Shooting averages
    avg_field_goal_pct: float
    avg_three_point_pct: float
    avg_free_throw_pct: float
    avg_true_shooting_pct: Optional[float]
    
    # Advanced metrics averages
    avg_player_efficiency_rating: Optional[float]
    avg_usage_rate: Optional[float]
    avg_defensive_impact_score: Optional[float]
    
    @property
    def month_year(self) -> str:
        """Return formatted month-year string."""
        return f"{self.year}-{self.month:02d}"


class TrendAnalyzer:
    """
    Analyzer for player performance trends over time.
    
    Provides monthly aggregation, recency weighting, trend detection,
    and statistical analysis of performance patterns.
    """
    
    def __init__(self, recency_decay: float = 0.95):
        """
        Initialize trend analyzer.
        
        Args:
            recency_decay: Exponential decay factor for recency weighting (0.0-1.0)
        """
        self.recency_decay = recency_decay
        self.monthly_data: Dict[str, MonthlyPerformance] = {}
        self.game_data: List[Tuple[date, PlayerGameStats]] = []
    
    def add_game(self, game_date: date, stats: PlayerGameStats) -> None:
        """Add a game to the trend analysis."""
        self.game_data.append((game_date, stats))
        self._update_monthly_aggregation(game_date, stats)
    
    def _update_monthly_aggregation(self, game_date: date, stats: PlayerGameStats) -> None:
        """Update monthly aggregated data with new game."""
        from .metrics import (
            calculate_true_shooting_percentage,
            calculate_player_efficiency_rating,
            calculate_usage_rate
        )
        from .defensive import calculate_defensive_impact_score
        
        month_key = f"{game_date.year}-{game_date.month:02d}"
        
        # Calculate advanced metrics for this game
        ts_pct = calculate_true_shooting_percentage(stats)
        per = calculate_player_efficiency_rating(stats)
        usage = calculate_usage_rate(stats)
        def_impact = calculate_defensive_impact_score(stats)
        
        if month_key not in self.monthly_data:
            # Create new monthly record
            self.monthly_data[month_key] = MonthlyPerformance(
                year=game_date.year,
                month=game_date.month,
                games_played=1,
                avg_points=float(stats.points),
                avg_rebounds=float(stats.rebounds_total),
                avg_assists=float(stats.assists),
                avg_steals=float(stats.steals),
                avg_blocks=float(stats.blocks),
                avg_turnovers=float(stats.turnovers),
                avg_minutes=stats.minutes_played,
                avg_field_goal_pct=stats.field_goals_attempted and (stats.field_goals_made / stats.field_goals_attempted) or 0.0,
                avg_three_point_pct=stats.three_pointers_attempted and (stats.three_pointers_made / stats.three_pointers_attempted) or 0.0,
                avg_free_throw_pct=stats.free_throws_attempted and (stats.free_throws_made / stats.free_throws_attempted) or 0.0,
                avg_true_shooting_pct=ts_pct,
                avg_player_efficiency_rating=per,
                avg_usage_rate=usage,
                avg_defensive_impact_score=def_impact
            )
        else:
            # Update existing monthly record with running average
            monthly = self.monthly_data[month_key]
            games = monthly.games_played
            new_games = games + 1
            
            # Update averages using running average formula
            monthly.avg_points = (monthly.avg_points * games + stats.points) / new_games
            monthly.avg_rebounds = (monthly.avg_rebounds * games + stats.rebounds_total) / new_games
            monthly.avg_assists = (monthly.avg_assists * games + stats.assists) / new_games
            monthly.avg_steals = (monthly.avg_steals * games + stats.steals) / new_games
            monthly.avg_blocks = (monthly.avg_blocks * games + stats.blocks) / new_games
            monthly.avg_turnovers = (monthly.avg_turnovers * games + stats.turnovers) / new_games
            monthly.avg_minutes = (monthly.avg_minutes * games + stats.minutes_played) / new_games
            
            # Shooting percentages
            fg_pct = stats.field_goals_attempted and (stats.field_goals_made / stats.field_goals_attempted) or 0.0
            monthly.avg_field_goal_pct = (monthly.avg_field_goal_pct * games + fg_pct) / new_games
            
            three_pct = stats.three_pointers_attempted and (stats.three_pointers_made / stats.three_pointers_attempted) or 0.0
            monthly.avg_three_point_pct = (monthly.avg_three_point_pct * games + three_pct) / new_games
            
            ft_pct = stats.free_throws_attempted and (stats.free_throws_made / stats.free_throws_attempted) or 0.0
            monthly.avg_free_throw_pct = (monthly.avg_free_throw_pct * games + ft_pct) / new_games
            
            # Advanced metrics (handle None values)
            if ts_pct is not None:
                if monthly.avg_true_shooting_pct is not None:
                    monthly.avg_true_shooting_pct = (monthly.avg_true_shooting_pct * games + ts_pct) / new_games
                else:
                    monthly.avg_true_shooting_pct = ts_pct
            
            if per is not None:
                if monthly.avg_player_efficiency_rating is not None:
                    monthly.avg_player_efficiency_rating = (monthly.avg_player_efficiency_rating * games + per) / new_games
                else:
                    monthly.avg_player_efficiency_rating = per
            
            if usage is not None:
                if monthly.avg_usage_rate is not None:
                    monthly.avg_usage_rate = (monthly.avg_usage_rate * games + usage) / new_games
                else:
                    monthly.avg_usage_rate = usage
            
            if def_impact is not None:
                if monthly.avg_defensive_impact_score is not None:
                    monthly.avg_defensive_impact_score = (monthly.avg_defensive_impact_score * games + def_impact) / new_games
                else:
                    monthly.avg_defensive_impact_score = def_impact
            
            monthly.games_played = new_games
    
    def calculate_weighted_average(self, metric: str) -> Optional[float]:
        """
        Calculate recency-weighted average for a specific metric.
        
        Args:
            metric: Name of the metric to calculate (e.g., 'avg_points')
            
        Returns:
            Weighted average value, None if no data
        """
        if not self.monthly_data:
            return None
        
        # Sort months chronologically (most recent first)
        sorted_months = sorted(
            self.monthly_data.items(),
            key=lambda x: (x[1].year, x[1].month),
            reverse=True
        )
        
        weighted_sum = 0.0
        weight_sum = 0.0
        
        for i, (month_key, monthly_perf) in enumerate(sorted_months):
            value = getattr(monthly_perf, metric, None)
            
            if value is not None:
                weight = (self.recency_decay ** i) * monthly_perf.games_played
                weighted_sum += value * weight
                weight_sum += weight
        
        return weighted_sum / weight_sum if weight_sum > 0 else None
    
    def detect_trend_direction(self, metric: str, min_months: int = 3) -> Optional[str]:
        """
        Detect trend direction for a specific metric using linear regression.
        
        Args:
            metric: Name of the metric to analyze
            min_months: Minimum number of months required for trend analysis
            
        Returns:
            "improving", "declining", "stable", or None if insufficient data
        """
        if len(self.monthly_data) < min_months:
            return None
        
        # Sort months chronologically (oldest first for regression)
        sorted_months = sorted(
            self.monthly_data.items(),
            key=lambda x: (x[1].year, x[1].month)
        )
        
        # Extract data points
        data_points = []
        for i, (month_key, monthly_perf) in enumerate(sorted_months):
            value = getattr(monthly_perf, metric, None)
            if value is not None:
                data_points.append((i, value))
        
        if len(data_points) < min_months:
            return None
        
        # Simple linear regression
        n = len(data_points)
        sum_x = sum(x for x, y in data_points)
        sum_y = sum(y for x, y in data_points)
        sum_xy = sum(x * y for x, y in data_points)
        sum_x2 = sum(x * x for x, y in data_points)
        
        # Calculate slope
        denominator = n * sum_x2 - sum_x * sum_x
        if denominator == 0:
            return "stable"
        
        slope = (n * sum_xy - sum_x * sum_y) / denominator
        
        # Determine significance threshold based on metric type
        if metric in ['avg_points', 'avg_rebounds', 'avg_assists']:
            threshold = 0.5  # 0.5 points/rebounds/assists per month
        elif metric in ['avg_field_goal_pct', 'avg_three_point_pct', 'avg_free_throw_pct', 'avg_true_shooting_pct']:
            threshold = 0.01  # 1% per month
        else:
            threshold = 0.1  # Default threshold
        
        if slope > threshold:
            return "improving"
        elif slope < -threshold:
            return "declining"
        else:
            return "stable"
    
    def get_recent_performance(self, months: int = 3) -> Dict[str, Any]:
        """
        Get performance summary for recent months.
        
        Args:
            months: Number of recent months to analyze
            
        Returns:
            Dictionary with recent performance analysis
        """
        if not self.monthly_data:
            return {'error': 'No monthly data available'}
        
        # Get most recent months
        sorted_months = sorted(
            self.monthly_data.items(),
            key=lambda x: (x[1].year, x[1].month),
            reverse=True
        )[:months]
        
        if not sorted_months:
            return {'error': 'No recent data available'}
        
        # Calculate averages for recent period
        recent_games = sum(monthly.games_played for _, monthly in sorted_months)
        
        # Weighted averages by games played
        total_weight = 0
        weighted_metrics = defaultdict(float)
        
        for month_key, monthly in sorted_months:
            weight = monthly.games_played
            total_weight += weight
            
            weighted_metrics['points'] += monthly.avg_points * weight
            weighted_metrics['rebounds'] += monthly.avg_rebounds * weight
            weighted_metrics['assists'] += monthly.avg_assists * weight
            weighted_metrics['field_goal_pct'] += monthly.avg_field_goal_pct * weight
            
            if monthly.avg_true_shooting_pct is not None:
                weighted_metrics['true_shooting_pct'] += monthly.avg_true_shooting_pct * weight
        
        # Calculate final averages
        if total_weight > 0:
            for metric in weighted_metrics:
                weighted_metrics[metric] /= total_weight
        
        return {
            'months_analyzed': len(sorted_months),
            'total_games': recent_games,
            'avg_points': round(weighted_metrics['points'], 1),
            'avg_rebounds': round(weighted_metrics['rebounds'], 1),
            'avg_assists': round(weighted_metrics['assists'], 1),
            'avg_field_goal_pct': round(weighted_metrics['field_goal_pct'], 3),
            'avg_true_shooting_pct': round(weighted_metrics['true_shooting_pct'], 3) if weighted_metrics['true_shooting_pct'] > 0 else None,
            'period': f"Last {months} month{'s' if months > 1 else ''}"
        }
    
    def get_trend_analysis_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive trend analysis summary.
        
        Returns:
            Dictionary with complete trend analysis
        """
        if not self.monthly_data:
            return {'error': 'No data available for trend analysis'}
        
        # Key metrics to analyze
        key_metrics = [
            'avg_points',
            'avg_rebounds', 
            'avg_assists',
            'avg_true_shooting_pct'
        ]
        
        trends = {}
        weighted_averages = {}
        
        for metric in key_metrics:
            trends[metric] = self.detect_trend_direction(metric)
            weighted_averages[metric] = self.calculate_weighted_average(metric)
        
        # Recent performance
        recent_3_months = self.get_recent_performance(3)
        
        return {
            'total_months': len(self.monthly_data),
            'total_games': sum(monthly.games_played for monthly in self.monthly_data.values()),
            'recency_decay_factor': self.recency_decay,
            'weighted_averages': {
                metric: round(value, 3) if value is not None else None
                for metric, value in weighted_averages.items()
            },
            'trend_directions': trends,
            'recent_performance': recent_3_months
        }