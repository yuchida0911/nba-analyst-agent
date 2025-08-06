"""Unit tests for advanced basketball metrics calculations."""

import pytest
from datetime import date

from nba_analyst.analytics.metrics import (
    PlayerGameStats,
    calculate_true_shooting_percentage,
    calculate_effective_field_goal_percentage,
    calculate_usage_rate,
    calculate_player_efficiency_rating,
    calculate_advanced_metrics_summary,
    validate_stats_for_metrics
)


class TestPlayerGameStats:
    """Test PlayerGameStats dataclass."""
    
    def test_default_values(self):
        """Test that default values are set correctly."""
        stats = PlayerGameStats()
        
        assert stats.points == 0
        assert stats.field_goals_made == 0
        assert stats.minutes_played == 0.0
        assert stats.rebounds_total == 0
    
    def test_custom_values(self):
        """Test custom values assignment."""
        stats = PlayerGameStats(
            points=25,
            field_goals_made=10,
            field_goals_attempted=18,
            minutes_played=32.5
        )
        
        assert stats.points == 25
        assert stats.field_goals_made == 10
        assert stats.field_goals_attempted == 18
        assert stats.minutes_played == 32.5


class TestTrueShootingPercentage:
    """Test True Shooting Percentage calculations."""
    
    def test_normal_calculation(self):
        """Test normal TS% calculation."""
        stats = PlayerGameStats(
            points=25,
            field_goals_attempted=18,
            free_throws_attempted=6
        )
        
        # TS% = 25 / (2 * (18 + 0.44 * 6)) = 25 / (2 * 20.64) = 25 / 41.28 ≈ 0.606
        ts_pct = calculate_true_shooting_percentage(stats)
        
        assert ts_pct is not None
        assert abs(ts_pct - 0.606) < 0.01
    
    def test_no_attempts(self):
        """Test TS% when no shots attempted."""
        stats = PlayerGameStats(
            points=0,
            field_goals_attempted=0,
            free_throws_attempted=0
        )
        
        ts_pct = calculate_true_shooting_percentage(stats)
        assert ts_pct is None
    
    def test_only_free_throws(self):
        """Test TS% with only free throw attempts."""
        stats = PlayerGameStats(
            points=8,
            field_goals_attempted=0,
            free_throws_attempted=10
        )
        
        # TS% = 8 / (2 * (0 + 0.44 * 10)) = 8 / 8.8 ≈ 0.909
        ts_pct = calculate_true_shooting_percentage(stats)
        
        assert ts_pct is not None
        assert abs(ts_pct - 0.909) < 0.01
    
    def test_perfect_efficiency(self):
        """Test TS% for perfect shooting efficiency."""
        stats = PlayerGameStats(
            points=50,  # 20 FG * 2 + 10 FT
            field_goals_attempted=20,
            free_throws_attempted=10
        )
        
        # TS% = 50 / (2 * (20 + 0.44 * 10)) = 50 / 48.8 ≈ 1.025
        ts_pct = calculate_true_shooting_percentage(stats)
        
        assert ts_pct is not None
        assert ts_pct > 1.0  # Can exceed 100% with high efficiency


class TestEffectiveFieldGoalPercentage:
    """Test Effective Field Goal Percentage calculations."""
    
    def test_normal_calculation(self):
        """Test normal eFG% calculation."""
        stats = PlayerGameStats(
            field_goals_made=10,
            field_goals_attempted=20,
            three_pointers_made=4
        )
        
        # eFG% = (10 + 0.5 * 4) / 20 = 12 / 20 = 0.6
        efg_pct = calculate_effective_field_goal_percentage(stats)
        
        assert efg_pct is not None
        assert abs(efg_pct - 0.6) < 0.01
    
    def test_no_attempts(self):
        """Test eFG% when no field goals attempted."""
        stats = PlayerGameStats(
            field_goals_made=0,
            field_goals_attempted=0,
            three_pointers_made=0
        )
        
        efg_pct = calculate_effective_field_goal_percentage(stats)
        assert efg_pct is None
    
    def test_no_three_pointers(self):
        """Test eFG% without three-pointers."""
        stats = PlayerGameStats(
            field_goals_made=8,
            field_goals_attempted=15,
            three_pointers_made=0
        )
        
        # eFG% = (8 + 0.5 * 0) / 15 = 8 / 15 ≈ 0.533
        efg_pct = calculate_effective_field_goal_percentage(stats)
        
        assert efg_pct is not None
        assert abs(efg_pct - 0.533) < 0.01
    
    def test_all_three_pointers(self):
        """Test eFG% when all field goals are three-pointers."""
        stats = PlayerGameStats(
            field_goals_made=6,
            field_goals_attempted=12,
            three_pointers_made=6
        )
        
        # eFG% = (6 + 0.5 * 6) / 12 = 9 / 12 = 0.75
        efg_pct = calculate_effective_field_goal_percentage(stats)
        
        assert efg_pct is not None
        assert abs(efg_pct - 0.75) < 0.01


class TestUsageRate:
    """Test Usage Rate calculations."""
    
    def test_normal_calculation(self):
        """Test normal usage rate calculation."""
        stats = PlayerGameStats(
            field_goals_attempted=18,
            free_throws_attempted=6,
            turnovers=3,
            minutes_played=36.0
        )
        
        # Simplified usage calculation
        usage_rate = calculate_usage_rate(stats)
        
        assert usage_rate is not None
        assert 0.0 <= usage_rate <= 1.0
    
    def test_no_minutes(self):
        """Test usage rate when no minutes played."""
        stats = PlayerGameStats(
            field_goals_attempted=10,
            free_throws_attempted=4,
            turnovers=2,
            minutes_played=0.0
        )
        
        usage_rate = calculate_usage_rate(stats)
        assert usage_rate is None
    
    def test_no_possessions_used(self):
        """Test usage rate when no possessions used."""
        stats = PlayerGameStats(
            field_goals_attempted=0,
            free_throws_attempted=0,
            turnovers=0,
            minutes_played=20.0
        )
        
        usage_rate = calculate_usage_rate(stats)
        assert usage_rate == 0.0
    
    def test_high_usage_game(self):
        """Test high usage rate scenario."""
        stats = PlayerGameStats(
            field_goals_attempted=30,
            free_throws_attempted=15,
            turnovers=8,
            minutes_played=42.0
        )
        
        usage_rate = calculate_usage_rate(stats)
        
        assert usage_rate is not None
        assert usage_rate <= 1.0  # Should be capped at 100%


class TestPlayerEfficiencyRating:
    """Test Player Efficiency Rating calculations."""
    
    def test_excellent_performance(self):
        """Test PER for excellent performance."""
        stats = PlayerGameStats(
            points=35,
            field_goals_made=12,
            field_goals_attempted=20,
            three_pointers_made=3,
            free_throws_made=8,
            free_throws_attempted=10,
            rebounds_total=10,
            assists=7,
            steals=2,
            blocks=1,
            turnovers=3,
            fouls_personal=2,
            minutes_played=35.0
        )
        
        per = calculate_player_efficiency_rating(stats)
        
        assert per is not None
        assert per >= 15.0  # Should be at or above league average
    
    def test_poor_performance(self):
        """Test PER for poor performance."""
        stats = PlayerGameStats(
            points=2,
            field_goals_made=1,
            field_goals_attempted=10,
            free_throws_made=0,
            free_throws_attempted=2,
            rebounds_total=1,
            assists=0,
            steals=0,
            blocks=0,
            turnovers=5,
            fouls_personal=6,
            minutes_played=20.0
        )
        
        per = calculate_player_efficiency_rating(stats)
        
        assert per is not None
        assert per >= 0.0  # Should be non-negative
        assert per < 10.0  # Should be well below average
    
    def test_no_minutes(self):
        """Test PER when no minutes played."""
        stats = PlayerGameStats(
            points=0,
            minutes_played=0.0
        )
        
        per = calculate_player_efficiency_rating(stats)
        assert per is None
    
    def test_dnp_game(self):
        """Test PER for DNP (Did Not Play) scenario."""
        stats = PlayerGameStats(
            points=0,
            field_goals_made=0,
            field_goals_attempted=0,
            minutes_played=0.0
        )
        
        per = calculate_player_efficiency_rating(stats)
        assert per is None


class TestAdvancedMetricsSummary:
    """Test advanced metrics summary function."""
    
    def test_complete_summary(self):
        """Test complete metrics summary."""
        stats = PlayerGameStats(
            points=25,
            field_goals_made=10,
            field_goals_attempted=18,
            three_pointers_made=3,
            three_pointers_attempted=8,
            free_throws_made=2,
            free_throws_attempted=3,
            rebounds_total=8,
            assists=6,
            steals=2,
            blocks=1,
            turnovers=3,
            minutes_played=32.0
        )
        
        summary = calculate_advanced_metrics_summary(stats)
        
        # Check all metrics are present
        assert 'true_shooting_percentage' in summary
        assert 'effective_field_goal_percentage' in summary
        assert 'usage_rate' in summary
        assert 'player_efficiency_rating' in summary
        assert 'minutes_played' in summary
        assert 'basic_stats' in summary
        
        # Check basic stats
        basic_stats = summary['basic_stats']
        assert basic_stats['points'] == 25
        assert basic_stats['rebounds'] == 8
        assert basic_stats['assists'] == 6
        
        # Check that metrics are calculated
        assert summary['true_shooting_percentage'] is not None
        assert summary['effective_field_goal_percentage'] is not None
        assert summary['usage_rate'] is not None
        assert summary['player_efficiency_rating'] is not None
    
    def test_dnp_summary(self):
        """Test summary for DNP player."""
        stats = PlayerGameStats(minutes_played=0.0)
        
        summary = calculate_advanced_metrics_summary(stats)
        
        # Most metrics should be None for DNP
        assert summary['true_shooting_percentage'] is None
        assert summary['usage_rate'] is None
        assert summary['player_efficiency_rating'] is None
        assert summary['minutes_played'] == 0.0


class TestStatsValidation:
    """Test statistics validation functions."""
    
    def test_valid_stats(self):
        """Test validation of valid statistics."""
        stats = PlayerGameStats(
            points=25,
            field_goals_made=10,
            field_goals_attempted=18,
            three_pointers_made=3,
            three_pointers_attempted=8,
            free_throws_made=2,
            free_throws_attempted=3,
            rebounds_offensive=2,
            rebounds_defensive=6,
            rebounds_total=8,
            minutes_played=32.0
        )
        
        errors = validate_stats_for_metrics(stats)
        assert len(errors) == 0
    
    def test_invalid_field_goals(self):
        """Test validation with invalid field goal data."""
        stats = PlayerGameStats(
            field_goals_made=15,  # More made than attempted
            field_goals_attempted=10
        )
        
        errors = validate_stats_for_metrics(stats)
        assert len(errors) > 0
        assert any("field goals made cannot exceed attempts" in error.lower() for error in errors)
    
    def test_invalid_three_pointers(self):
        """Test validation with invalid three-pointer data."""
        stats = PlayerGameStats(
            three_pointers_made=8,  # More made than attempted
            three_pointers_attempted=5
        )
        
        errors = validate_stats_for_metrics(stats)
        assert len(errors) > 0
        assert any("three pointers made cannot exceed attempts" in error.lower() for error in errors)
    
    def test_invalid_rebounds(self):
        """Test validation with invalid rebound data."""
        stats = PlayerGameStats(
            rebounds_offensive=3,
            rebounds_defensive=5,
            rebounds_total=10  # Should be 8
        )
        
        errors = validate_stats_for_metrics(stats)
        assert len(errors) > 0
        assert any("rebounds" in error.lower() for error in errors)
    
    def test_negative_minutes(self):
        """Test validation with negative minutes."""
        stats = PlayerGameStats(minutes_played=-5.0)
        
        errors = validate_stats_for_metrics(stats)
        assert len(errors) > 0
        assert any("minutes played cannot be negative" in error.lower() for error in errors)
    
    def test_three_pointers_exceed_field_goals(self):
        """Test validation when three-pointers exceed field goals."""
        stats = PlayerGameStats(
            field_goals_made=5,
            three_pointers_made=8  # Cannot exceed total field goals
        )
        
        errors = validate_stats_for_metrics(stats)
        assert len(errors) > 0
        assert any("three pointers made cannot exceed total field goals" in error.lower() for error in errors)