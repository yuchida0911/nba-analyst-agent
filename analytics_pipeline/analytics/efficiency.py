"""
Shooting and Scoring Efficiency Analysis

This module provides comprehensive analysis of player shooting efficiency,
including True Shooting percentage analysis, efficiency grading, and
trend detection for scoring performance.
"""

from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass
from datetime import date
import statistics
from .metrics import PlayerGameStats, calculate_true_shooting_percentage


@dataclass 
class EfficiencyGame:
    """Single game efficiency data point."""
    
    game_date: date
    true_shooting_pct: float
    points: int
    field_goal_attempts: int
    minutes_played: float
    
    
class EfficiencyAnalyzer:
    """
    Analyzer for player shooting and scoring efficiency.
    
    Provides methods to analyze efficiency trends, grade performance,
    and identify strengths/weaknesses in shooting.
    """
    
    def __init__(self):
        self.efficiency_games: List[EfficiencyGame] = []
    
    def add_game(self, game_data: EfficiencyGame) -> None:
        """Add a game to the efficiency analysis."""
        self.efficiency_games.append(game_data)
    
    def add_game_from_stats(self, game_date: date, stats: PlayerGameStats) -> None:
        """Add a game from PlayerGameStats object."""
        ts_pct = calculate_true_shooting_percentage(stats)
        if ts_pct is not None:
            game = EfficiencyGame(
                game_date=game_date,
                true_shooting_pct=ts_pct,
                points=stats.points,
                field_goal_attempts=stats.field_goals_attempted,
                minutes_played=stats.minutes_played
            )
            self.add_game(game)
    
    def calculate_efficiency_trend(self, recency_weight: float = 0.95) -> Optional[float]:
        """
        Calculate weighted efficiency trend over time.
        
        Args:
            recency_weight: Exponential decay factor for recent games (0.0-1.0)
            
        Returns:
            Weighted average True Shooting percentage, None if no data
        """
        if not self.efficiency_games:
            return None
        
        # Sort games by date (most recent first)
        sorted_games = sorted(self.efficiency_games, key=lambda x: x.game_date, reverse=True)
        
        weighted_sum = 0.0
        weight_sum = 0.0
        
        for i, game in enumerate(sorted_games):
            weight = recency_weight ** i
            weighted_sum += game.true_shooting_pct * weight
            weight_sum += weight
        
        return weighted_sum / weight_sum if weight_sum > 0 else None
    
    def detect_efficiency_trend_direction(self, window_size: int = 10) -> Optional[str]:
        """
        Detect if efficiency is trending up, down, or stable.
        
        Args:
            window_size: Number of recent games to analyze
            
        Returns:
            "improving", "declining", "stable", or None if insufficient data
        """
        if len(self.efficiency_games) < window_size:
            return None
        
        # Get recent games
        sorted_games = sorted(self.efficiency_games, key=lambda x: x.game_date, reverse=True)
        recent_games = sorted_games[:window_size]
        
        # Calculate first half vs second half averages
        mid_point = window_size // 2
        recent_half = [g.true_shooting_pct for g in recent_games[:mid_point]]
        earlier_half = [g.true_shooting_pct for g in recent_games[mid_point:]]
        
        if not recent_half or not earlier_half:
            return None
        
        recent_avg = statistics.mean(recent_half)
        earlier_avg = statistics.mean(earlier_half)
        
        # Determine trend with 2% threshold for significance
        diff = recent_avg - earlier_avg
        
        if diff > 0.02:  # 2% improvement
            return "improving"
        elif diff < -0.02:  # 2% decline
            return "declining"
        else:
            return "stable"
    
    def grade_efficiency(self, ts_percentage: float) -> str:
        """
        Assign letter grade to True Shooting percentage.
        
        Based on NBA efficiency standards:
        - Elite: 60%+
        - Very Good: 55-60%
        - Good: 52-55%
        - Average: 50-52%
        - Below Average: 47-50%
        - Poor: <47%
        
        Args:
            ts_percentage: True Shooting percentage as decimal (0.0-1.0)
            
        Returns:
            Letter grade (A+ to D)
        """
        pct = ts_percentage * 100  # Convert to percentage
        
        if pct >= 62:
            return "A+"
        elif pct >= 60:
            return "A"
        elif pct >= 57:
            return "A-"
        elif pct >= 55:
            return "B+"
        elif pct >= 52:
            return "B"
        elif pct >= 50:
            return "B-"
        elif pct >= 47:
            return "C+"
        elif pct >= 45:
            return "C"
        elif pct >= 42:
            return "C-"
        elif pct >= 40:
            return "D+"
        elif pct >= 37:
            return "D"
        else:
            return "D-"
    
    def calculate_consistency_score(self) -> Optional[float]:
        """
        Calculate shooting consistency score based on standard deviation.
        
        Lower standard deviation = higher consistency
        
        Returns:
            Consistency score (0-100), where 100 is most consistent
        """
        if len(self.efficiency_games) < 3:
            return None
        
        ts_percentages = [game.true_shooting_pct for game in self.efficiency_games]
        
        if not ts_percentages:
            return None
        
        std_dev = statistics.stdev(ts_percentages)
        mean_ts = statistics.mean(ts_percentages)
        
        # Calculate coefficient of variation
        if mean_ts == 0:
            return 0.0
        
        cv = std_dev / mean_ts
        
        # Convert to consistency score (lower CV = higher consistency)
        # Scale so that CV of 0.3 = score of 70, CV of 0.1 = score of 90
        consistency_score = max(100 - (cv * 300), 0)
        
        return min(consistency_score, 100.0)
    
    def analyze_volume_vs_efficiency(self) -> Dict[str, Any]:
        """
        Analyze relationship between shot volume and efficiency.
        
        Returns:
            Dictionary with volume/efficiency analysis
        """
        if not self.efficiency_games:
            return {'error': 'No games available for analysis'}
        
        # Calculate averages
        avg_fga = statistics.mean([g.field_goal_attempts for g in self.efficiency_games])
        avg_ts = statistics.mean([g.true_shooting_pct for g in self.efficiency_games])
        avg_points = statistics.mean([g.points for g in self.efficiency_games])
        
        # Categorize volume
        if avg_fga >= 15:
            volume_category = "High Volume"
        elif avg_fga >= 10:
            volume_category = "Medium Volume"
        else:
            volume_category = "Low Volume"
        
        # Analyze high vs low volume games
        high_volume_games = [g for g in self.efficiency_games if g.field_goal_attempts >= avg_fga]
        low_volume_games = [g for g in self.efficiency_games if g.field_goal_attempts < avg_fga]
        
        high_vol_efficiency = None
        low_vol_efficiency = None
        
        if high_volume_games:
            high_vol_efficiency = statistics.mean([g.true_shooting_pct for g in high_volume_games])
        
        if low_volume_games:
            low_vol_efficiency = statistics.mean([g.true_shooting_pct for g in low_volume_games])
        
        return {
            'volume_category': volume_category,
            'avg_field_goal_attempts': round(avg_fga, 1),
            'avg_true_shooting_pct': round(avg_ts, 3),
            'avg_points_per_game': round(avg_points, 1),
            'high_volume_efficiency': round(high_vol_efficiency, 3) if high_vol_efficiency else None,
            'low_volume_efficiency': round(low_vol_efficiency, 3) if low_vol_efficiency else None,
            'efficiency_grade': self.grade_efficiency(avg_ts),
            'total_games': len(self.efficiency_games)
        }
    
    def get_efficiency_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive efficiency analysis summary.
        
        Returns:
            Dictionary with complete efficiency analysis
        """
        if not self.efficiency_games:
            return {'error': 'No games available for analysis'}
        
        # Basic calculations
        avg_ts = statistics.mean([g.true_shooting_pct for g in self.efficiency_games])
        weighted_ts = self.calculate_efficiency_trend()
        trend_direction = self.detect_efficiency_trend_direction()
        consistency = self.calculate_consistency_score()
        volume_analysis = self.analyze_volume_vs_efficiency()
        
        # Best and worst games
        best_game = max(self.efficiency_games, key=lambda x: x.true_shooting_pct)
        worst_game = min(self.efficiency_games, key=lambda x: x.true_shooting_pct)
        
        return {
            'games_analyzed': len(self.efficiency_games),
            'average_true_shooting_pct': round(avg_ts, 3),
            'weighted_true_shooting_pct': round(weighted_ts, 3) if weighted_ts else None,
            'efficiency_grade': self.grade_efficiency(avg_ts),
            'trend_direction': trend_direction,
            'consistency_score': round(consistency, 1) if consistency else None,
            'volume_analysis': volume_analysis,
            'best_game': {
                'date': best_game.game_date.isoformat(),
                'true_shooting_pct': round(best_game.true_shooting_pct, 3),
                'points': best_game.points
            },
            'worst_game': {
                'date': worst_game.game_date.isoformat(),
                'true_shooting_pct': round(worst_game.true_shooting_pct, 3),
                'points': worst_game.points
            }
        }