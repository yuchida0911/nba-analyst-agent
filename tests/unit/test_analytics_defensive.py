"""Unit tests for defensive analytics calculations."""

import pytest

from nba_analyst.analytics.defensive import (
    calculate_defensive_impact_score,
    calculate_steal_rate,
    calculate_block_rate,
    calculate_defensive_rebound_rate,
    calculate_foul_efficiency,
    grade_defensive_performance,
    analyze_defensive_strengths
)
from nba_analyst.analytics.metrics import PlayerGameStats


class TestDefensiveImpactScore:
    """Test Defensive Impact Score calculations."""
    
    def test_excellent_defense(self):
        """Test defensive impact score for excellent defense."""
        stats = PlayerGameStats(
            steals=4,
            blocks=3,
            rebounds_defensive=12,
            fouls_personal=1,
            minutes_played=36.0
        )
        
        impact_score = calculate_defensive_impact_score(stats)
        
        assert impact_score is not None
        assert impact_score > 75.0  # Should be high for excellent defense
        assert impact_score <= 100.0  # Should not exceed maximum
    
    def test_poor_defense(self):
        """Test defensive impact score for poor defense."""
        stats = PlayerGameStats(
            steals=0,
            blocks=0,
            rebounds_defensive=2,
            fouls_personal=6,
            minutes_played=25.0
        )
        
        impact_score = calculate_defensive_impact_score(stats)
        
        assert impact_score is not None
        assert impact_score < 30.0  # Should be low for poor defense
        assert impact_score >= 0.0  # Should not be negative
    
    def test_no_minutes(self):
        """Test defensive impact score when no minutes played."""
        stats = PlayerGameStats(
            steals=2,
            blocks=1,
            rebounds_defensive=5,
            minutes_played=0.0
        )
        
        impact_score = calculate_defensive_impact_score(stats)
        assert impact_score is None
    
    def test_average_defense(self):
        """Test defensive impact score for average defense."""
        stats = PlayerGameStats(
            steals=1,
            blocks=1,
            rebounds_defensive=6,
            fouls_personal=3,
            minutes_played=30.0
        )
        
        impact_score = calculate_defensive_impact_score(stats)
        
        assert impact_score is not None
        assert 30.0 <= impact_score <= 70.0  # Should be in middle range
    
    def test_minutes_factor_bonus(self):
        """Test that more minutes played provides a bonus."""
        stats_less_minutes = PlayerGameStats(
            steals=2,
            blocks=1,
            rebounds_defensive=8,
            fouls_personal=2,
            minutes_played=20.0
        )
        
        stats_more_minutes = PlayerGameStats(
            steals=2,
            blocks=1,
            rebounds_defensive=8,
            fouls_personal=2,
            minutes_played=35.0
        )
        
        score_less = calculate_defensive_impact_score(stats_less_minutes)
        score_more = calculate_defensive_impact_score(stats_more_minutes)
        
        assert score_less is not None
        assert score_more is not None
        assert score_more > score_less  # More minutes should give higher score


class TestDefensiveRates:
    """Test individual defensive rate calculations."""
    
    def test_steal_rate(self):
        """Test steal rate calculation."""
        stats = PlayerGameStats(
            steals=3,
            minutes_played=36.0
        )
        
        steal_rate = calculate_steal_rate(stats)
        
        assert steal_rate is not None
        assert steal_rate == 3.0  # 3 steals in 36 minutes = 3.0 per 36
    
    def test_steal_rate_partial_minutes(self):
        """Test steal rate with partial minutes."""
        stats = PlayerGameStats(
            steals=2,
            minutes_played=24.0
        )
        
        steal_rate = calculate_steal_rate(stats)
        
        assert steal_rate is not None
        assert steal_rate == 3.0  # 2 steals in 24 minutes = 3.0 per 36
    
    def test_block_rate(self):
        """Test block rate calculation."""
        stats = PlayerGameStats(
            blocks=2,
            minutes_played=30.0
        )
        
        block_rate = calculate_block_rate(stats)
        
        assert block_rate is not None
        assert abs(block_rate - 2.4) < 0.01  # 2 blocks in 30 minutes = 2.4 per 36
    
    def test_defensive_rebound_rate(self):
        """Test defensive rebound rate calculation."""
        stats = PlayerGameStats(
            rebounds_defensive=9,
            minutes_played=36.0
        )
        
        dreb_rate = calculate_defensive_rebound_rate(stats)
        
        assert dreb_rate is not None
        assert dreb_rate == 9.0  # 9 defensive rebounds in 36 minutes
    
    def test_foul_efficiency(self):
        """Test foul efficiency calculation."""
        stats = PlayerGameStats(
            fouls_personal=4,
            minutes_played=32.0
        )
        
        foul_efficiency = calculate_foul_efficiency(stats)
        
        assert foul_efficiency is not None
        assert abs(foul_efficiency - 4.5) < 0.01  # 4 fouls in 32 minutes = 4.5 per 36
    
    def test_rates_no_minutes(self):
        """Test that all rates return None when no minutes played."""
        stats = PlayerGameStats(
            steals=2,
            blocks=1,
            rebounds_defensive=5,
            fouls_personal=3,
            minutes_played=0.0
        )
        
        assert calculate_steal_rate(stats) is None
        assert calculate_block_rate(stats) is None
        assert calculate_defensive_rebound_rate(stats) is None
        assert calculate_foul_efficiency(stats) is None


class TestDefensiveGrading:
    """Test defensive performance grading."""
    
    def test_a_plus_grade(self):
        """Test A+ grade for excellent defense."""
        grade = grade_defensive_performance(90.0)
        assert grade == "A+"
    
    def test_a_grade(self):
        """Test A grade."""
        grade = grade_defensive_performance(82.0)
        assert grade == "A"
    
    def test_b_grades(self):
        """Test B-level grades."""
        assert grade_defensive_performance(77.0) == "A-"
        assert grade_defensive_performance(72.0) == "B+"
        assert grade_defensive_performance(67.0) == "B"
        assert grade_defensive_performance(62.0) == "B-"
    
    def test_c_grades(self):
        """Test C-level grades."""
        assert grade_defensive_performance(57.0) == "C+"
        assert grade_defensive_performance(52.0) == "C"
        assert grade_defensive_performance(47.0) == "C-"
    
    def test_d_grades(self):
        """Test D-level grades."""
        assert grade_defensive_performance(42.0) == "D+"
        assert grade_defensive_performance(37.0) == "D"
        assert grade_defensive_performance(30.0) == "D-"
    
    def test_edge_cases(self):
        """Test edge cases for grading."""
        assert grade_defensive_performance(85.0) == "A+"  # Exactly at threshold
        assert grade_defensive_performance(84.9) == "A"   # Just below threshold
        assert grade_defensive_performance(0.0) == "D-"   # Minimum score
        assert grade_defensive_performance(100.0) == "A+" # Maximum score


class TestDefensiveStrengthsAnalysis:
    """Test comprehensive defensive strengths analysis."""
    
    def test_excellent_all_around_defense(self):
        """Test analysis for excellent all-around defense."""
        stats = PlayerGameStats(
            steals=3,
            blocks=2,
            rebounds_defensive=10,
            fouls_personal=2,
            minutes_played=36.0
        )
        
        analysis = analyze_defensive_strengths(stats)
        
        assert analysis['defensive_impact_score'] is not None
        assert analysis['grade'] is not None
        assert analysis['defensive_impact_score'] > 70.0
        assert len(analysis['strengths']) >= 3  # Should have multiple strengths
        assert len(analysis['weaknesses']) == 0  # Should have no weaknesses
    
    def test_steal_specialist(self):
        """Test analysis for steal specialist."""
        stats = PlayerGameStats(
            steals=4,  # High steal rate
            blocks=0,
            rebounds_defensive=3,
            fouls_personal=3,
            minutes_played=36.0
        )
        
        analysis = analyze_defensive_strengths(stats)
        
        assert "steal" in str(analysis['strengths']).lower()
        assert analysis['steal_rate_per_36'] >= 3.5
    
    def test_shot_blocker(self):
        """Test analysis for shot blocker."""
        stats = PlayerGameStats(
            steals=1,
            blocks=4,  # High block rate
            rebounds_defensive=8,
            fouls_personal=2,
            minutes_played=36.0
        )
        
        analysis = analyze_defensive_strengths(stats)
        
        assert "block" in str(analysis['strengths']).lower()
        assert analysis['block_rate_per_36'] >= 3.0
    
    def test_rebounding_specialist(self):
        """Test analysis for rebounding specialist."""
        stats = PlayerGameStats(
            steals=1,
            blocks=1,
            rebounds_defensive=12,  # High rebounding
            fouls_personal=3,
            minutes_played=36.0
        )
        
        analysis = analyze_defensive_strengths(stats)
        
        assert "rebounding" in str(analysis['strengths']).lower()
        assert analysis['defensive_rebound_rate_per_36'] >= 10.0
    
    def test_disciplined_defender(self):
        """Test analysis for disciplined defender (low fouls)."""
        stats = PlayerGameStats(
            steals=2,
            blocks=1,
            rebounds_defensive=6,
            fouls_personal=1,  # Very low fouls
            minutes_played=36.0
        )
        
        analysis = analyze_defensive_strengths(stats)
        
        assert "disciplined" in str(analysis['strengths']).lower()
        assert analysis['foul_rate_per_36'] <= 2.0
    
    def test_poor_defense_weaknesses(self):
        """Test analysis identifies weaknesses in poor defense."""
        stats = PlayerGameStats(
            steals=0,  # Low steals
            blocks=0,  # No blocks
            rebounds_defensive=2,  # Poor rebounding
            fouls_personal=6,  # High fouls
            minutes_played=30.0
        )
        
        analysis = analyze_defensive_strengths(stats)
        
        assert len(analysis['weaknesses']) >= 3  # Should identify multiple weaknesses
        assert analysis['defensive_impact_score'] < 40.0
        assert analysis['grade'] in ["D+", "D", "D-"]
    
    def test_no_minutes_error(self):
        """Test analysis with no minutes played."""
        stats = PlayerGameStats(minutes_played=0.0)
        
        analysis = analyze_defensive_strengths(stats)
        
        assert 'error' in analysis
        assert 'insufficient playing time' in analysis['error'].lower()
    
    def test_analysis_structure(self):
        """Test that analysis returns all expected fields."""
        stats = PlayerGameStats(
            steals=2,
            blocks=1,
            rebounds_defensive=7,
            fouls_personal=3,
            minutes_played=32.0
        )
        
        analysis = analyze_defensive_strengths(stats)
        
        # Check all expected fields are present
        expected_fields = [
            'defensive_impact_score',
            'grade',
            'steal_rate_per_36',
            'block_rate_per_36',
            'defensive_rebound_rate_per_36',
            'foul_rate_per_36',
            'strengths',
            'weaknesses',
            'minutes_played'
        ]
        
        for field in expected_fields:
            assert field in analysis
        
        # Check types
        assert isinstance(analysis['strengths'], list)
        assert isinstance(analysis['weaknesses'], list)
        assert isinstance(analysis['minutes_played'], float)