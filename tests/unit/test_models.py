"""Unit tests for database models."""

import pytest
from datetime import date

from analytics_pipeline.database.models import PlayerBoxScore, TeamGameTotal


class TestPlayerBoxScore:
    """Test cases for PlayerBoxScore model."""
    
    def test_player_box_score_creation(self):
        """Test creating PlayerBoxScore instance."""
        player = PlayerBoxScore(
            game_id=12345,
            person_id=2544,
            season_year='2023-24',
            game_date=date(2024, 1, 15),
            team_id=1610612747,
            team_city='Los Angeles',
            team_name='Lakers',
            team_tricode='LAL',
            team_slug='lakers',
            person_name='LeBron James',
            position='F',
            jersey_num='6',
            minutes='35:24',
            points=35,
            assists=7,
            rebounds_total=10,
            rebounds_offensive=2,
            rebounds_defensive=8,
            field_goals_made=12,
            field_goals_attempted=20,
            three_pointers_made=3,
            three_pointers_attempted=8,
            free_throws_made=8,
            free_throws_attempted=10
        )
        
        assert player.game_id == 12345
        assert player.person_id == 2544
        assert player.season_year == '2023-24'
        assert player.person_name == 'LeBron James'
        assert player.points == 35
        assert player.assists == 7
        assert player.rebounds_total == 10
    
    def test_player_box_score_repr(self):
        """Test PlayerBoxScore string representation."""
        player = PlayerBoxScore(
            game_id=12345,
            person_id=2544,
            person_name='LeBron James',
            points=35
        )
        
        repr_str = repr(player)
        assert 'PlayerBoxScore' in repr_str
        assert 'game_id=12345' in repr_str
        assert 'person_id=2544' in repr_str
        assert 'LeBron James' in repr_str
        assert 'points=35' in repr_str
    
    def test_minutes_decimal_conversion(self):
        """Test minutes conversion from MM:SS to decimal."""
        player = PlayerBoxScore(minutes='35:24')
        assert player.minutes_decimal == 35 + 24/60  # 35.4
        
        player = PlayerBoxScore(minutes='12:30')
        assert player.minutes_decimal == 12.5
        
        player = PlayerBoxScore(minutes='0')
        assert player.minutes_decimal == 0.0
        
        player = PlayerBoxScore(minutes='25.5')  # Already decimal
        assert player.minutes_decimal == 25.5
        
        player = PlayerBoxScore(minutes='invalid')
        assert player.minutes_decimal is None
        
        player = PlayerBoxScore(minutes=None)
        assert player.minutes_decimal == 0.0
    
    def test_is_dnp_property(self):
        """Test DNP (Did Not Play) detection."""
        # DNP cases
        player1 = PlayerBoxScore(minutes='0')
        assert player1.is_dnp is True
        
        player2 = PlayerBoxScore(minutes='0:00')
        assert player2.is_dnp is True
        
        player3 = PlayerBoxScore(minutes='', comment='DNP - Coach\'s Decision')
        assert player3.is_dnp is True
        
        player4 = PlayerBoxScore(minutes=None)
        assert player4.is_dnp is True
        
        # Played cases
        player5 = PlayerBoxScore(minutes='25:30')
        assert player5.is_dnp is False
        
        player6 = PlayerBoxScore(minutes='0:01')  # Even 1 second counts
        assert player6.is_dnp is False
    
    def test_data_integrity_validation_rebounds(self):
        """Test data integrity validation for rebounds."""
        # Valid rebounds
        player = PlayerBoxScore(
            rebounds_offensive=5,
            rebounds_defensive=8,
            rebounds_total=13
        )
        errors = player.validate_data_integrity()
        assert len(errors) == 0
        
        # Invalid rebounds
        player = PlayerBoxScore(
            rebounds_offensive=5,
            rebounds_defensive=8,
            rebounds_total=10  # Should be 13
        )
        errors = player.validate_data_integrity()
        assert len(errors) > 0
        assert any('rebounds' in error.lower() for error in errors)
    
    def test_data_integrity_validation_shooting(self):
        """Test data integrity validation for shooting stats."""
        # Valid shooting
        player = PlayerBoxScore(
            field_goals_made=8,
            field_goals_attempted=15,
            three_pointers_made=3,
            three_pointers_attempted=7,
            free_throws_made=6,
            free_throws_attempted=8
        )
        errors = player.validate_data_integrity()
        assert len(errors) == 0
        
        # Invalid: FGM > FGA
        player = PlayerBoxScore(
            field_goals_made=10,
            field_goals_attempted=8  # Less than made
        )
        errors = player.validate_data_integrity()
        assert len(errors) > 0
        assert any('field goals made' in error.lower() for error in errors)
        
        # Invalid: 3PM > FGM
        player = PlayerBoxScore(
            field_goals_made=5,
            three_pointers_made=8  # More than total FG
        )
        errors = player.validate_data_integrity()
        assert len(errors) > 0
        assert any('three pointers made' in error.lower() for error in errors)
    
    def test_data_integrity_validation_negative_values(self):
        """Test data integrity validation for negative values."""
        # Valid non-negative stats
        player = PlayerBoxScore(
            points=25,
            assists=8,
            steals=3,
            blocks=1,
            turnovers=2
        )
        errors = player.validate_data_integrity()
        assert len(errors) == 0
        
        # Invalid negative stats
        player = PlayerBoxScore(
            points=-5,
            assists=-2,
            steals=3
        )
        errors = player.validate_data_integrity()
        assert len(errors) >= 2  # At least 2 negative value errors
        assert any('points' in error and 'negative' in error for error in errors)
        assert any('assists' in error and 'negative' in error for error in errors)
    
    def test_data_integrity_validation_with_none_values(self):
        """Test data integrity validation with None values."""
        player = PlayerBoxScore(
            rebounds_offensive=None,
            rebounds_defensive=None,
            rebounds_total=None,
            field_goals_made=None,
            field_goals_attempted=None
        )
        
        # Should not crash with None values
        errors = player.validate_data_integrity()
        # May or may not have errors, but should not raise exception
        assert isinstance(errors, list)


class TestTeamGameTotal:
    """Test cases for TeamGameTotal model."""
    
    def test_team_game_total_creation(self):
        """Test creating TeamGameTotal instance."""
        team = TeamGameTotal(
            game_id=22300123,
            team_id=1610612747,
            season_year='2023-24',
            team_abbreviation='LAL',
            team_name='Los Angeles Lakers',
            game_date=date(2024, 1, 15),
            matchup='LAL @ GSW',
            wl='W',
            pts=123,
            fgm=45,
            fga=88,
            reb=45,
            ast=28
        )
        
        assert team.game_id == 22300123
        assert team.team_id == 1610612747
        assert team.season_year == '2023-24'
        assert team.team_name == 'Los Angeles Lakers'
        assert team.pts == 123
        assert team.wl == 'W'
    
    def test_team_game_total_repr(self):
        """Test TeamGameTotal string representation."""
        team = TeamGameTotal(
            game_id=22300123,
            team_id=1610612747,
            team_name='Los Angeles Lakers',
            pts=123,
            wl='W'
        )
        
        repr_str = repr(team)
        assert 'TeamGameTotal' in repr_str
        assert 'game_id=22300123' in repr_str
        assert 'team_id=1610612747' in repr_str
        assert 'Los Angeles Lakers' in repr_str
        assert 'pts=123' in repr_str
        assert "wl='W'" in repr_str
    
    def test_is_win_property(self):
        """Test is_win property."""
        team_win = TeamGameTotal(wl='W')
        assert team_win.is_win is True
        assert team_win.is_loss is False
        
        team_loss = TeamGameTotal(wl='L')
        assert team_loss.is_win is False
        assert team_loss.is_loss is True
    
    def test_is_loss_property(self):
        """Test is_loss property."""
        team_loss = TeamGameTotal(wl='L')
        assert team_loss.is_loss is True
        assert team_loss.is_win is False
        
        team_win = TeamGameTotal(wl='W')
        assert team_win.is_loss is False
        assert team_win.is_win is True


class TestModelsIntegration:
    """Integration tests for model interactions."""
    
    @pytest.mark.database
    def test_create_player_box_score_in_db(self, test_db_session):
        """Test creating PlayerBoxScore in database."""
        player = PlayerBoxScore(
            game_id=12345,
            person_id=2544,
            season_year='2023-24',
            game_date=date(2024, 1, 15),
            team_id=1610612747,
            team_city='Los Angeles',
            team_name='Lakers',
            team_tricode='LAL',
            team_slug='lakers',
            person_name='Test Player',
            points=25,
            assists=8
        )
        
        test_db_session.add(player)
        test_db_session.commit()
        
        # Verify it was saved
        saved_player = test_db_session.query(PlayerBoxScore).filter_by(
            game_id=12345, person_id=2544
        ).first()
        
        assert saved_player is not None
        assert saved_player.person_name == 'Test Player'
        assert saved_player.points == 25
        assert saved_player.assists == 8
    
    @pytest.mark.database
    def test_create_team_game_total_in_db(self, test_db_session):
        """Test creating TeamGameTotal in database."""
        team = TeamGameTotal(
            game_id=22300123,
            team_id=1610612747,
            season_year='2023-24',
            team_abbreviation='LAL',
            team_name='Test Lakers',
            game_date=date(2024, 1, 15),
            matchup='LAL @ GSW',
            wl='W',
            min_played=48.0,
            pts=120,
            fgm=45,
            fga=90,
            reb=50,
            ast=25,
            tov=12.0,
            stl=8,
            blk=5,
            blka=3,
            pf=18,
            pfd=20,
            fg_pct=0.5,
            fg3m=12,
            fg3a=30,
            fg3_pct=0.4,
            ftm=18,
            fta=22,
            ft_pct=0.818,
            oreb=12,
            dreb=38,
            plus_minus=10.0,
            available_flag=1.0
        )
        
        test_db_session.add(team)
        test_db_session.commit()
        
        # Verify it was saved
        saved_team = test_db_session.query(TeamGameTotal).filter_by(
            game_id=22300123, team_id=1610612747
        ).first()
        
        assert saved_team is not None
        assert saved_team.team_name == 'Test Lakers'
        assert saved_team.pts == 120
        assert saved_team.wl == 'W'
        assert saved_team.is_win is True