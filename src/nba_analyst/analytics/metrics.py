"""
Core Advanced Basketball Metrics

This module implements advanced basketball analytics metrics including:
- True Shooting Percentage (TS%)
- Effective Field Goal Percentage (eFG%)
- Usage Rate estimation
- Player Efficiency Rating (PER)

All metrics follow standard basketball analytics formulas and best practices.
"""

from typing import Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class PlayerGameStats:
    """Container for player game statistics needed for advanced metrics."""
    
    # Basic stats
    points: int = 0
    field_goals_made: int = 0
    field_goals_attempted: int = 0
    three_pointers_made: int = 0
    three_pointers_attempted: int = 0
    free_throws_made: int = 0
    free_throws_attempted: int = 0
    
    # Rebounds and other stats
    rebounds_offensive: int = 0
    rebounds_defensive: int = 0
    rebounds_total: int = 0
    assists: int = 0
    steals: int = 0
    blocks: int = 0
    turnovers: int = 0
    fouls_personal: int = 0
    
    # Playing time (in decimal minutes)
    minutes_played: float = 0.0
    
    # Team context (needed for some calculations)
    team_field_goals_attempted: Optional[int] = None
    team_turnovers: Optional[int] = None
    team_offensive_rebounds: Optional[int] = None
    team_free_throws_attempted: Optional[int] = None


def calculate_true_shooting_percentage(stats: PlayerGameStats) -> Optional[float]:
    """
    Calculate True Shooting Percentage (TS%).
    
    TS% = Points / (2 * True Shooting Attempts)
    True Shooting Attempts = FGA + 0.44 * FTA
    
    Args:
        stats: Player game statistics
        
    Returns:
        True shooting percentage as decimal (0.0-1.0), None if cannot calculate
    """
    if stats.field_goals_attempted == 0 and stats.free_throws_attempted == 0:
        return None
        
    true_shooting_attempts = stats.field_goals_attempted + (0.44 * stats.free_throws_attempted)
    
    if true_shooting_attempts == 0:
        return None
        
    return stats.points / (2 * true_shooting_attempts)


def calculate_effective_field_goal_percentage(stats: PlayerGameStats) -> Optional[float]:
    """
    Calculate Effective Field Goal Percentage (eFG%).
    
    eFG% = (FGM + 0.5 * 3PM) / FGA
    
    Args:
        stats: Player game statistics
        
    Returns:
        Effective field goal percentage as decimal (0.0-1.0), None if cannot calculate
    """
    if stats.field_goals_attempted == 0:
        return None
        
    effective_field_goals = stats.field_goals_made + (0.5 * stats.three_pointers_made)
    return effective_field_goals / stats.field_goals_attempted


def calculate_usage_rate(stats: PlayerGameStats) -> Optional[float]:
    """
    Calculate Usage Rate estimation.
    
    Usage Rate estimates the percentage of team plays used by a player while on court.
    This is a simplified version that doesn't require full team data.
    
    Usage% ≈ (FGA + 0.44 * FTA + TOV) / (Minutes Played / 5) / Team Pace Factor
    
    Args:
        stats: Player game statistics
        
    Returns:
        Usage rate as decimal (0.0-1.0), None if cannot calculate
    """
    if stats.minutes_played <= 0:
        return None
        
    # Simplified usage calculation without full team context
    # Uses player's shot attempts and turnovers as proxy for possessions used
    player_possessions = (
        stats.field_goals_attempted + 
        (0.44 * stats.free_throws_attempted) + 
        stats.turnovers
    )
    
    if player_possessions == 0:
        return 0.0
        
    # Estimate team possessions based on minutes played
    # Assumes ~100 possessions per 48 minutes (typical NBA pace)
    estimated_team_possessions = (stats.minutes_played / 48.0) * 100.0
    
    if estimated_team_possessions <= 0:
        return None
        
    return min(player_possessions / estimated_team_possessions, 1.0)


def calculate_player_efficiency_rating(stats: PlayerGameStats) -> Optional[float]:
    """
    Calculate Player Efficiency Rating (PER) - simplified version.
    
    This is a simplified PER calculation that doesn't require full league averages.
    True PER requires extensive league context and pace adjustments.
    
    Simplified PER focuses on per-minute production across all statistical categories.
    
    Args:
        stats: Player game statistics
        
    Returns:
        Simplified PER value, None if cannot calculate
    """
    if stats.minutes_played <= 0:
        return None
        
    # Positive contributions
    positive_stats = (
        stats.field_goals_made +
        (0.5 * stats.three_pointers_made) +  # Bonus for 3-pointers
        stats.free_throws_made +
        stats.rebounds_offensive +
        stats.rebounds_defensive +
        stats.assists +
        stats.steals +
        stats.blocks
    )
    
    # Negative contributions
    negative_stats = (
        (stats.field_goals_attempted - stats.field_goals_made) +  # Missed FG
        (stats.free_throws_attempted - stats.free_throws_made) +  # Missed FT
        stats.turnovers +
        (0.5 * stats.fouls_personal)  # Half weight for fouls
    )
    
    # Net contribution per minute
    net_contribution = positive_stats - negative_stats
    per_minute_rating = net_contribution / stats.minutes_played
    
    # Scale to traditional PER-like range (league average ~15)
    # Use higher multiplier to match expected PER ranges
    scaled_per = per_minute_rating * 30.0
    
    # Ensure non-negative result
    return max(scaled_per, 0.0)


def calculate_advanced_metrics_summary(stats: PlayerGameStats) -> Dict[str, Any]:
    """
    Calculate all advanced metrics for a player's game performance.
    
    Args:
        stats: Player game statistics
        
    Returns:
        Dictionary containing all calculated advanced metrics
    """
    return {
        'true_shooting_percentage': calculate_true_shooting_percentage(stats),
        'effective_field_goal_percentage': calculate_effective_field_goal_percentage(stats),
        'usage_rate': calculate_usage_rate(stats),
        'player_efficiency_rating': calculate_player_efficiency_rating(stats),
        'minutes_played': stats.minutes_played,
        'basic_stats': {
            'points': stats.points,
            'rebounds': stats.rebounds_total,
            'assists': stats.assists,
            'steals': stats.steals,
            'blocks': stats.blocks,
            'turnovers': stats.turnovers
        }
    }


def validate_stats_for_metrics(stats: PlayerGameStats) -> list[str]:
    """
    Validate player stats for metrics calculation.
    
    Args:
        stats: Player game statistics to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Check for negative values
    if stats.minutes_played < 0:
        errors.append("Minutes played cannot be negative")
        
    if stats.field_goals_made > stats.field_goals_attempted:
        errors.append("Field goals made cannot exceed attempts")
        
    if stats.three_pointers_made > stats.three_pointers_attempted:
        errors.append("Three pointers made cannot exceed attempts")
        
    if stats.free_throws_made > stats.free_throws_attempted:
        errors.append("Free throws made cannot exceed attempts")
        
    if stats.three_pointers_made > stats.field_goals_made:
        errors.append("Three pointers made cannot exceed total field goals made")
        
    if stats.three_pointers_attempted > stats.field_goals_attempted:
        errors.append("Three pointers attempted cannot exceed total field goal attempts")
        
    # Check rebounds consistency
    if (stats.rebounds_total is not None and 
        stats.rebounds_offensive is not None and 
        stats.rebounds_defensive is not None):
        if stats.rebounds_total != (stats.rebounds_offensive + stats.rebounds_defensive):
            errors.append("Total rebounds should equal offensive + defensive rebounds")
    
    return errors