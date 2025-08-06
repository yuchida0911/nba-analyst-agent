"""
Defensive Impact Analysis

This module implements defensive metrics and impact scoring for NBA players.
The Defensive Impact Score is a composite metric that evaluates a player's
overall defensive contribution using multiple statistical categories.
"""

from typing import Optional, Dict, Any
from dataclasses import dataclass
from .metrics import PlayerGameStats


@dataclass
class DefensiveStats:
    """Container for defensive-specific statistics."""
    
    # Direct defensive stats
    steals: int = 0
    blocks: int = 0
    defensive_rebounds: int = 0
    fouls_personal: int = 0
    
    # Context stats
    minutes_played: float = 0.0
    opponent_field_goal_percentage: Optional[float] = None
    plus_minus: Optional[int] = None
    
    # Team context (for advanced calculations)
    team_defensive_rating: Optional[float] = None
    team_steals: Optional[int] = None
    team_blocks: Optional[int] = None


def calculate_defensive_impact_score(stats: PlayerGameStats) -> Optional[float]:
    """
    Calculate Defensive Impact Score - a composite defensive metric.
    
    This score combines multiple defensive statistics into a single metric
    that estimates a player's overall defensive contribution.
    
    Components:
    - Steals rate (steals per minute)
    - Blocks rate (blocks per minute) 
    - Defensive rebounding rate
    - Foul efficiency (low fouls = better)
    - Minutes played weighting
    
    Args:
        stats: Player game statistics
        
    Returns:
        Defensive Impact Score (0-100 scale), None if cannot calculate
    """
    if stats.minutes_played <= 0:
        return None
        
    # Component 1: Steals rate (per 36 minutes)
    steals_per_36 = (stats.steals / stats.minutes_played) * 36.0
    steals_score = min(steals_per_36 * 8.0, 25.0)  # Cap at 25 points
    
    # Component 2: Blocks rate (per 36 minutes)
    blocks_per_36 = (stats.blocks / stats.minutes_played) * 36.0
    blocks_score = min(blocks_per_36 * 6.0, 20.0)  # Cap at 20 points
    
    # Component 3: Defensive rebounding rate
    dreb_per_36 = (stats.rebounds_defensive / stats.minutes_played) * 36.0
    dreb_score = min(dreb_per_36 * 2.0, 25.0)  # Cap at 25 points
    
    # Component 4: Foul efficiency (lower fouls = higher score)
    if stats.fouls_personal == 0:
        foul_score = 15.0
    else:
        fouls_per_36 = (stats.fouls_personal / stats.minutes_played) * 36.0
        # Invert: fewer fouls = higher score
        foul_score = max(15.0 - (fouls_per_36 * 2.0), 0.0)
    
    # Component 5: Minutes played factor (more minutes = slight bonus)
    minutes_factor = min(stats.minutes_played / 32.0, 1.2)  # Cap at 20% bonus
    
    # Combine components
    base_score = steals_score + blocks_score + dreb_score + foul_score
    final_score = base_score * minutes_factor
    
    # Scale to 0-100 range
    return min(final_score, 100.0)


def calculate_steal_rate(stats: PlayerGameStats) -> Optional[float]:
    """
    Calculate steal rate (steals per 36 minutes).
    
    Args:
        stats: Player game statistics
        
    Returns:
        Steals per 36 minutes, None if cannot calculate
    """
    if stats.minutes_played <= 0:
        return None
        
    return (stats.steals / stats.minutes_played) * 36.0


def calculate_block_rate(stats: PlayerGameStats) -> Optional[float]:
    """
    Calculate block rate (blocks per 36 minutes).
    
    Args:
        stats: Player game statistics
        
    Returns:
        Blocks per 36 minutes, None if cannot calculate
    """
    if stats.minutes_played <= 0:
        return None
        
    return (stats.blocks / stats.minutes_played) * 36.0


def calculate_defensive_rebound_rate(stats: PlayerGameStats) -> Optional[float]:
    """
    Calculate defensive rebound rate (defensive rebounds per 36 minutes).
    
    Args:
        stats: Player game statistics
        
    Returns:
        Defensive rebounds per 36 minutes, None if cannot calculate
    """
    if stats.minutes_played <= 0:
        return None
        
    return (stats.rebounds_defensive / stats.minutes_played) * 36.0


def calculate_foul_efficiency(stats: PlayerGameStats) -> Optional[float]:
    """
    Calculate foul efficiency (lower is better).
    
    Measures how many fouls a player commits per minute of play.
    
    Args:
        stats: Player game statistics
        
    Returns:
        Fouls per 36 minutes, None if cannot calculate
    """
    if stats.minutes_played <= 0:
        return None
        
    return (stats.fouls_personal / stats.minutes_played) * 36.0


def grade_defensive_performance(defensive_impact_score: float) -> str:
    """
    Assign letter grade to defensive performance based on impact score.
    
    Args:
        defensive_impact_score: Calculated defensive impact score (0-100)
        
    Returns:
        Letter grade (A+ to D)
    """
    if defensive_impact_score >= 85:
        return "A+"
    elif defensive_impact_score >= 80:
        return "A"
    elif defensive_impact_score >= 75:
        return "A-"
    elif defensive_impact_score >= 70:
        return "B+"
    elif defensive_impact_score >= 65:
        return "B"
    elif defensive_impact_score >= 60:
        return "B-"
    elif defensive_impact_score >= 55:
        return "C+"
    elif defensive_impact_score >= 50:
        return "C"
    elif defensive_impact_score >= 45:
        return "C-"
    elif defensive_impact_score >= 40:
        return "D+"
    elif defensive_impact_score >= 35:
        return "D"
    else:
        return "D-"


def analyze_defensive_strengths(stats: PlayerGameStats) -> Dict[str, Any]:
    """
    Analyze a player's defensive strengths and weaknesses.
    
    Args:
        stats: Player game statistics
        
    Returns:
        Dictionary with defensive analysis
    """
    if stats.minutes_played <= 0:
        return {'error': 'Insufficient playing time for analysis'}
    
    # Calculate individual components
    steal_rate = calculate_steal_rate(stats)
    block_rate = calculate_block_rate(stats)
    dreb_rate = calculate_defensive_rebound_rate(stats)
    foul_efficiency = calculate_foul_efficiency(stats)
    impact_score = calculate_defensive_impact_score(stats)
    
    # Identify strengths (top percentile thresholds)
    strengths = []
    weaknesses = []
    
    # Steal rate analysis
    if steal_rate and steal_rate >= 2.0:
        strengths.append("Excellent steal rate")
    elif steal_rate and steal_rate < 0.8:
        weaknesses.append("Low steal production")
    elif steal_rate == 0:
        weaknesses.append("No steals")
    
    # Block rate analysis  
    if block_rate and block_rate >= 1.5:
        strengths.append("Strong shot blocking")
    elif block_rate and block_rate < 0.3:
        weaknesses.append("Limited shot blocking")
    elif block_rate == 0:
        weaknesses.append("No blocks")
    # Defensive rebounding analysis
    if dreb_rate and dreb_rate >= 8.0:
        strengths.append("Excellent defensive rebounding")
    elif dreb_rate and dreb_rate < 4.0:
        weaknesses.append("Below average defensive rebounding")
    elif dreb_rate == 0:
        weaknesses.append("No defensive rebounds")
    # Foul efficiency analysis
    if foul_efficiency and foul_efficiency <= 3.0:
        strengths.append("Disciplined defense (low fouls)")
    elif foul_efficiency and foul_efficiency >= 6.0:
        weaknesses.append("Foul prone")
    elif foul_efficiency == 0:
        weaknesses.append("No fouls")
    return {
        'defensive_impact_score': impact_score,
        'grade': grade_defensive_performance(impact_score) if impact_score else None,
        'steal_rate_per_36': steal_rate,
        'block_rate_per_36': block_rate,
        'defensive_rebound_rate_per_36': dreb_rate,
        'foul_rate_per_36': foul_efficiency,
        'strengths': strengths,
        'weaknesses': weaknesses,
        'minutes_played': stats.minutes_played
    }