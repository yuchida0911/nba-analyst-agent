"""Unit tests for shooting efficiency analysis."""

import pytest
from datetime import date

from nba_analyst.analytics.efficiency import (
    EfficiencyGame,
    EfficiencyAnalyzer
)
from nba_analyst.analytics.metrics import PlayerGameStats


class TestEfficiencyGame:
    """Test EfficiencyGame dataclass."""
    
    def test_creation(self):
        """Test EfficiencyGame creation."""
        game = EfficiencyGame(
            game_date=date(2024, 1, 15),
            true_shooting_pct=0.625,
            points=25,
            field_goal_attempts=18,
            minutes_played=32.5
        )
        
        assert game.game_date == date(2024, 1, 15)
        assert game.true_shooting_pct == 0.625
        assert game.points == 25
        assert game.field_goal_attempts == 18
        assert game.minutes_played == 32.5


class TestEfficiencyAnalyzer:
    """Test EfficiencyAnalyzer class."""
    
    def test_initialization(self):
        """Test analyzer initialization."""
        analyzer = EfficiencyAnalyzer()
        assert len(analyzer.efficiency_games) == 0
    
    def test_add_game(self):
        """Test adding a game to the analyzer."""
        analyzer = EfficiencyAnalyzer()
        game = EfficiencyGame(
            game_date=date(2024, 1, 15),
            true_shooting_pct=0.600,
            points=20,
            field_goal_attempts=15,
            minutes_played=30.0
        )
        
        analyzer.add_game(game)
        assert len(analyzer.efficiency_games) == 1
        assert analyzer.efficiency_games[0] == game
    
    def test_add_game_from_stats(self):
        """Test adding a game from PlayerGameStats."""
        analyzer = EfficiencyAnalyzer()
        stats = PlayerGameStats(
            points=25,
            field_goals_attempted=18,
            free_throws_attempted=6,
            minutes_played=32.0
        )
        
        analyzer.add_game_from_stats(date(2024, 1, 15), stats)
        
        assert len(analyzer.efficiency_games) == 1
        game = analyzer.efficiency_games[0]
        assert game.points == 25
        assert game.field_goal_attempts == 18
        assert game.minutes_played == 32.0
    
    def test_add_game_from_stats_invalid(self):
        """Test adding invalid stats doesn't add a game."""
        analyzer = EfficiencyAnalyzer()
        stats = PlayerGameStats(
            points=0,
            field_goals_attempted=0,
            free_throws_attempted=0,
            minutes_played=0.0
        )
        
        analyzer.add_game_from_stats(date(2024, 1, 15), stats)
        
        # Should not add game since TS% is None
        assert len(analyzer.efficiency_games) == 0


class TestEfficiencyTrend:
    """Test efficiency trend calculations."""
    
    def test_efficiency_trend_single_game(self):
        """Test efficiency trend with single game."""
        analyzer = EfficiencyAnalyzer()
        game = EfficiencyGame(
            game_date=date(2024, 1, 15),
            true_shooting_pct=0.600,
            points=20,
            field_goal_attempts=15,
            minutes_played=30.0
        )
        analyzer.add_game(game)
        
        trend = analyzer.calculate_efficiency_trend()
        assert trend == 0.600
    
    def test_efficiency_trend_multiple_games(self):
        """Test efficiency trend with multiple games."""
        analyzer = EfficiencyAnalyzer()
        
        # Add games with different dates and efficiencies
        games = [
            EfficiencyGame(date(2024, 1, 10), 0.500, 15, 12, 25.0),  # Oldest
            EfficiencyGame(date(2024, 1, 15), 0.600, 20, 15, 30.0),  # Middle
            EfficiencyGame(date(2024, 1, 20), 0.700, 25, 18, 35.0),  # Most recent
        ]
        
        for game in games:
            analyzer.add_game(game)
        
        trend = analyzer.calculate_efficiency_trend()
        
        # Should be weighted toward recent games
        assert trend is not None
        assert trend > 0.600  # Should be higher than simple average due to recency weighting
    
    def test_efficiency_trend_no_games(self):
        """Test efficiency trend with no games."""
        analyzer = EfficiencyAnalyzer()
        trend = analyzer.calculate_efficiency_trend()
        assert trend is None
    
    def test_efficiency_trend_custom_recency(self):
        """Test efficiency trend with custom recency weight."""
        analyzer = EfficiencyAnalyzer()
        
        games = [
            EfficiencyGame(date(2024, 1, 10), 0.400, 15, 12, 25.0),  # Oldest
            EfficiencyGame(date(2024, 1, 20), 0.800, 25, 18, 35.0),  # Most recent
        ]
        
        for game in games:
            analyzer.add_game(game)
        
        # Test with high recency weight (recent games matter more)
        trend_high_recency = analyzer.calculate_efficiency_trend(recency_weight=0.9)
        
        # Test with low recency weight (all games weighted more equally)
        trend_low_recency = analyzer.calculate_efficiency_trend(recency_weight=0.5)
        
        # High recency should weight recent game (0.800) more heavily than low recency
        # So high_recency should be closer to 0.800, low_recency closer to 0.600 (average)
        assert trend_high_recency != trend_low_recency  # Should be different


class TestTrendDirection:
    """Test trend direction detection."""
    
    def test_improving_trend(self):
        """Test detection of improving trend."""
        analyzer = EfficiencyAnalyzer()
        
        # Add games showing improvement over time
        dates = [date(2024, 1, i) for i in range(1, 15)]  # 14 games
        ts_pcts = [0.450 + (i * 0.020) for i in range(14)]  # Improving from 45% to 71%
        
        for i, (game_date, ts_pct) in enumerate(zip(dates, ts_pcts)):
            game = EfficiencyGame(game_date, ts_pct, 20, 15, 30.0)
            analyzer.add_game(game)
        
        trend_direction = analyzer.detect_efficiency_trend_direction()
        assert trend_direction == "improving"
    
    def test_declining_trend(self):
        """Test detection of declining trend."""
        analyzer = EfficiencyAnalyzer()
        
        # Add games showing decline over time
        dates = [date(2024, 1, i) for i in range(1, 15)]  # 14 games
        ts_pcts = [0.700 - (i * 0.020) for i in range(14)]  # Declining from 70% to 44%
        
        for i, (game_date, ts_pct) in enumerate(zip(dates, ts_pcts)):
            game = EfficiencyGame(game_date, ts_pct, 20, 15, 30.0)
            analyzer.add_game(game)
        
        trend_direction = analyzer.detect_efficiency_trend_direction()
        assert trend_direction == "declining"
    
    def test_stable_trend(self):
        """Test detection of stable trend."""
        analyzer = EfficiencyAnalyzer()
        
        # Add games with stable efficiency
        dates = [date(2024, 1, i) for i in range(1, 15)]  # 14 games
        ts_pcts = [0.580] * 14  # Consistent 58%
        
        for game_date, ts_pct in zip(dates, ts_pcts):
            game = EfficiencyGame(game_date, ts_pct, 20, 15, 30.0)
            analyzer.add_game(game)
        
        trend_direction = analyzer.detect_efficiency_trend_direction()
        assert trend_direction == "stable"
    
    def test_insufficient_games_for_trend(self):
        """Test trend direction with insufficient games."""
        analyzer = EfficiencyAnalyzer()
        
        # Add only 5 games (default window is 10)
        for i in range(5):
            game = EfficiencyGame(date(2024, 1, i+1), 0.600, 20, 15, 30.0)
            analyzer.add_game(game)
        
        trend_direction = analyzer.detect_efficiency_trend_direction()
        assert trend_direction is None


class TestEfficiencyGrading:
    """Test efficiency grading system."""
    
    def test_a_plus_grade(self):
        """Test A+ grade for elite efficiency."""
        analyzer = EfficiencyAnalyzer()
        grade = analyzer.grade_efficiency(0.625)  # 62.5%
        assert grade == "A+"
    
    def test_grade_progression(self):
        """Test grade progression through all levels."""
        analyzer = EfficiencyAnalyzer()
        
        # Test various efficiency levels (aligned with actual grading thresholds)
        test_cases = [
            (0.630, "A+"),
            (0.600, "A"),
            (0.575, "A-"),
            (0.550, "B+"),
            (0.520, "B"),
            (0.500, "B-"),
            (0.470, "C+"),
            (0.450, "C"),
            (0.420, "C-"),
            (0.400, "D+"),
            (0.370, "D"),
            (0.300, "D-")
        ]
        
        for ts_pct, expected_grade in test_cases:
            grade = analyzer.grade_efficiency(ts_pct)
            assert grade == expected_grade, f"Expected {expected_grade} for {ts_pct:.1%}, got {grade}"


class TestConsistencyScore:
    """Test shooting consistency calculations."""
    
    def test_perfect_consistency(self):
        """Test consistency score for perfect consistency."""
        analyzer = EfficiencyAnalyzer()
        
        # Add games with identical efficiency
        for i in range(10):
            game = EfficiencyGame(date(2024, 1, i+1), 0.600, 20, 15, 30.0)
            analyzer.add_game(game)
        
        consistency = analyzer.calculate_consistency_score()
        assert consistency is not None
        assert consistency > 95.0  # Should be very high for perfect consistency
    
    def test_inconsistent_shooting(self):
        """Test consistency score for inconsistent shooting."""
        analyzer = EfficiencyAnalyzer()
        
        # Add games with highly variable efficiency
        efficiencies = [0.300, 0.800, 0.200, 0.900, 0.100, 0.950, 0.150, 0.850]
        for i, eff in enumerate(efficiencies):
            game = EfficiencyGame(date(2024, 1, i+1), eff, 20, 15, 30.0)
            analyzer.add_game(game)
        
        consistency = analyzer.calculate_consistency_score()
        assert consistency is not None
        assert consistency < 50.0  # Should be low for inconsistent shooting
    
    def test_insufficient_games_for_consistency(self):
        """Test consistency score with insufficient games."""
        analyzer = EfficiencyAnalyzer()
        
        # Add only 2 games (need at least 3 for std dev)
        for i in range(2):
            game = EfficiencyGame(date(2024, 1, i+1), 0.600, 20, 15, 30.0)
            analyzer.add_game(game)
        
        consistency = analyzer.calculate_consistency_score()
        assert consistency is None


class TestVolumeAnalysis:
    """Test volume vs efficiency analysis."""
    
    def test_high_volume_analysis(self):
        """Test analysis for high-volume shooter."""
        analyzer = EfficiencyAnalyzer()
        
        # Add high-volume games
        for i in range(10):
            game = EfficiencyGame(date(2024, 1, i+1), 0.550, 25, 18, 35.0)  # 18 FGA = high volume
            analyzer.add_game(game)
        
        analysis = analyzer.analyze_volume_vs_efficiency()
        
        assert analysis['volume_category'] == "High Volume"
        assert analysis['avg_field_goal_attempts'] >= 15.0
    
    def test_low_volume_analysis(self):
        """Test analysis for low-volume shooter."""
        analyzer = EfficiencyAnalyzer()
        
        # Add low-volume games
        for i in range(10):
            game = EfficiencyGame(date(2024, 1, i+1), 0.650, 12, 8, 20.0)  # 8 FGA = low volume
            analyzer.add_game(game)
        
        analysis = analyzer.analyze_volume_vs_efficiency()
        
        assert analysis['volume_category'] == "Low Volume"
        assert analysis['avg_field_goal_attempts'] < 10.0
    
    def test_efficiency_comparison_by_volume(self):
        """Test efficiency comparison between high and low volume games."""
        analyzer = EfficiencyAnalyzer()
        
        # Add mix of high and low volume games
        # High volume games with lower efficiency
        for i in range(5):
            game = EfficiencyGame(date(2024, 1, i+1), 0.500, 20, 20, 35.0)  # High volume, lower efficiency
            analyzer.add_game(game)
        
        # Low volume games with higher efficiency
        for i in range(5):
            game = EfficiencyGame(date(2024, 1, i+6), 0.700, 12, 8, 20.0)  # Low volume, higher efficiency
            analyzer.add_game(game)
        
        analysis = analyzer.analyze_volume_vs_efficiency()
        
        assert analysis['high_volume_efficiency'] is not None
        assert analysis['low_volume_efficiency'] is not None
        assert analysis['low_volume_efficiency'] > analysis['high_volume_efficiency']


class TestEfficiencySummary:
    """Test comprehensive efficiency summary."""
    
    def test_complete_summary(self):
        """Test complete efficiency summary."""
        analyzer = EfficiencyAnalyzer()
        
        # Add variety of games
        efficiencies = [0.450, 0.520, 0.580, 0.610, 0.490, 0.650, 0.540, 0.600, 0.570, 0.590]
        for i, eff in enumerate(efficiencies):
            game = EfficiencyGame(date(2024, 1, i+1), eff, 18, 14, 30.0)
            analyzer.add_game(game)
        
        summary = analyzer.get_efficiency_summary()
        
        # Check all expected fields
        expected_fields = [
            'games_analyzed',
            'average_true_shooting_pct',
            'weighted_true_shooting_pct',
            'efficiency_grade',
            'trend_direction',
            'consistency_score',
            'volume_analysis',
            'best_game',
            'worst_game'
        ]
        
        for field in expected_fields:
            assert field in summary
        
        # Check values
        assert summary['games_analyzed'] == 10
        assert summary['best_game']['true_shooting_pct'] == 0.650
        assert summary['worst_game']['true_shooting_pct'] == 0.450
        assert summary['volume_analysis']['volume_category'] in ["High Volume", "Medium Volume", "Low Volume"]
    
    def test_empty_summary(self):
        """Test summary with no games."""
        analyzer = EfficiencyAnalyzer()
        summary = analyzer.get_efficiency_summary()
        
        assert 'error' in summary
        assert 'no games available' in summary['error'].lower()