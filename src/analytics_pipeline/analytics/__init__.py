"""
NBA Analytics Module

This module provides advanced basketball analytics and metrics calculation
for player and team performance analysis.

Main Components:
- metrics: Core basketball metrics (TS%, PER, Usage Rate, etc.)
- defensive: Defensive impact calculations
- trends: Monthly trend analysis and recency weighting
- efficiency: Shooting and scoring efficiency analysis
"""

from .metrics import (
    calculate_true_shooting_percentage,
    calculate_effective_field_goal_percentage,
    calculate_usage_rate,
    calculate_player_efficiency_rating
)

from .defensive import calculate_defensive_impact_score
from .efficiency import EfficiencyAnalyzer
from .trends import TrendAnalyzer

__all__ = [
    'calculate_true_shooting_percentage',
    'calculate_effective_field_goal_percentage', 
    'calculate_usage_rate',
    'calculate_player_efficiency_rating',
    'calculate_defensive_impact_score',
    'EfficiencyAnalyzer',
    'TrendAnalyzer'
]