"""
SQLAlchemy models for NBA Analyst Agent database.

This module defines the database schema using SQLAlchemy ORM models
based on the JSON schema specifications for NBA data.
"""

from datetime import date
from typing import Optional, Dict

from sqlalchemy import Column, Integer, String, Date, Float, Text, Boolean, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import PrimaryKeyConstraint, Index

# Base class for all models
Base = declarative_base()


class PlayerBoxScore(Base):
    """
    Player-level game statistics (box scores) table.
    
    Based on the box_scores_schema.json specification.
    Raw data imported directly from CSV files.
    """
    
    __tablename__ = 'players_raw'
    
    # Primary key fields (composite key)
    game_id = Column('gameId', BigInteger, nullable=False, comment="Unique identifier for each NBA game")
    person_id = Column('personId', Integer, nullable=False, comment="Unique identifier for the NBA player")
    
    # Game and team information
    season_year = Column(String(7), nullable=False, comment="The NBA season year (e.g., '2010-11')")
    game_date = Column(Date, nullable=False, comment="Date when the game was played")
    matchup = Column(String(20), nullable=True, comment="Team matchup (e.g., 'NJN @ CLE')")
    
    # Team information
    team_id = Column('teamId', BigInteger, nullable=False, comment="Unique identifier for the NBA team")
    team_city = Column('teamCity', String(50), nullable=False, comment="City where the team is based")
    team_name = Column('teamName', String(50), nullable=False, comment="Official team name")
    team_tricode = Column('teamTricode', String(3), nullable=False, comment="Three-letter team abbreviation")
    team_slug = Column('teamSlug', String(20), nullable=False, comment="URL-friendly team identifier")
    
    # Player information
    person_name = Column('personName', String(100), nullable=False, comment="Full name of the player")
    position = Column(String(10), nullable=True, comment="Player's position (G, F, C, etc.)")
    comment = Column(Text, nullable=True, comment="Special notes (DNP reasons, injuries, etc.)")
    jersey_num = Column('jerseyNum', String(3), nullable=True, comment="Player's jersey number")
    
    # Game statistics - playing time
    minutes = Column(String(10), nullable=True, comment="Total minutes played in MM:SS format")
    
    # Game statistics - shooting
    field_goals_made = Column('fieldGoalsMade', Integer, nullable=False, default=0, comment="Field goals made")
    field_goals_attempted = Column('fieldGoalsAttempted', Integer, nullable=False, default=0, comment="Field goal attempts")
    field_goals_percentage = Column('fieldGoalsPercentage', Float, nullable=False, default=0.0, comment="Field goal percentage")
    
    three_pointers_made = Column('threePointersMade', Integer, nullable=False, default=0, comment="Three-point shots made")
    three_pointers_attempted = Column('threePointersAttempted', Integer, nullable=False, default=0, comment="Three-point attempts")
    three_pointers_percentage = Column('threePointersPercentage', Float, nullable=False, default=0.0, comment="Three-point percentage")
    
    free_throws_made = Column('freeThrowsMade', Integer, nullable=False, default=0, comment="Free throws made")
    free_throws_attempted = Column('freeThrowsAttempted', Integer, nullable=False, default=0, comment="Free throw attempts")
    free_throws_percentage = Column('freeThrowsPercentage', Float, nullable=False, default=0.0, comment="Free throw percentage")
    
    # Game statistics - rebounds
    rebounds_offensive = Column('reboundsOffensive', Integer, nullable=False, default=0, comment="Offensive rebounds")
    rebounds_defensive = Column('reboundsDefensive', Integer, nullable=False, default=0, comment="Defensive rebounds")
    rebounds_total = Column('reboundsTotal', Integer, nullable=False, default=0, comment="Total rebounds")
    
    # Game statistics - other
    assists = Column(Integer, nullable=False, default=0, comment="Assists")
    steals = Column(Integer, nullable=False, default=0, comment="Steals")
    blocks = Column(Integer, nullable=False, default=0, comment="Blocked shots")
    turnovers = Column(Integer, nullable=False, default=0, comment="Turnovers")
    fouls_personal = Column('foulsPersonal', Integer, nullable=False, default=0, comment="Personal fouls")
    points = Column(Integer, nullable=False, default=0, comment="Total points scored")
    plus_minus_points = Column('plusMinusPoints', Integer, nullable=False, default=0, comment="Plus-minus statistic")
    
    # Define composite primary key
    __table_args__ = (
        PrimaryKeyConstraint('gameId', 'personId', name='pk_players_raw'),
        
        # Indexes for common queries
        Index('idx_players_raw_person_date', 'personId', 'game_date'),
        Index('idx_players_raw_game', 'gameId'),
        Index('idx_players_raw_team_date', 'teamId', 'game_date'),
        Index('idx_players_raw_season', 'season_year'),
        Index('idx_players_raw_person_season', 'personId', 'season_year'),
        
        {
            'comment': 'Raw player box score data imported from CSV files'
        }
    )
    
    def __repr__(self) -> str:
        """String representation of the model."""
        return (
            f"<PlayerBoxScore(game_id={self.game_id}, person_id={self.person_id}, "
            f"person_name='{self.person_name}', points={self.points})>"
        )
    
    @property
    def minutes_decimal(self) -> Optional[float]:
        """Convert MM:SS minutes format to decimal minutes."""
        if not self.minutes or self.minutes == "0":
            return 0.0
        
        try:
            if ":" in self.minutes:
                mm, ss = self.minutes.split(":")
                return int(mm) + int(ss) / 60.0
            else:
                return float(self.minutes)
        except (ValueError, TypeError):
            return None
    
    @property
    def is_dnp(self) -> bool:
        """Check if player did not play (DNP)."""
        return (
            self.minutes in ("0", "0:00", "", None) or
            (self.comment is not None and "DNP" in self.comment)
        )
    
    def validate_data_integrity(self) -> list[str]:
        """
        Validate data integrity according to business rules.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Rebounds validation
        if (self.rebounds_total is not None and 
            self.rebounds_offensive is not None and 
            self.rebounds_defensive is not None and
            self.rebounds_total != (self.rebounds_offensive + self.rebounds_defensive)):
            errors.append(f"Total rebounds ({self.rebounds_total}) != offensive ({self.rebounds_offensive}) + defensive ({self.rebounds_defensive})")
        
        # Shooting validation
        if (self.field_goals_made is not None and self.field_goals_attempted is not None and
            self.field_goals_made > self.field_goals_attempted):
            errors.append(f"Field goals made ({self.field_goals_made}) > attempted ({self.field_goals_attempted})")
        
        if (self.three_pointers_made is not None and self.three_pointers_attempted is not None and
            self.three_pointers_made > self.three_pointers_attempted):
            errors.append(f"Three pointers made ({self.three_pointers_made}) > attempted ({self.three_pointers_attempted})")
        
        if (self.three_pointers_made is not None and self.field_goals_made is not None and
            self.three_pointers_made > self.field_goals_made):
            errors.append(f"Three pointers made ({self.three_pointers_made}) > field goals made ({self.field_goals_made})")
        
        if (self.three_pointers_attempted is not None and self.field_goals_attempted is not None and
            self.three_pointers_attempted > self.field_goals_attempted):
            errors.append(f"Three pointers attempted ({self.three_pointers_attempted}) > field goals attempted ({self.field_goals_attempted})")
        
        if (self.free_throws_made is not None and self.free_throws_attempted is not None and
            self.free_throws_made > self.free_throws_attempted):
            errors.append(f"Free throws made ({self.free_throws_made}) > attempted ({self.free_throws_attempted})")
        
        # Negative values validation
        numeric_fields = [
            'field_goals_made', 'field_goals_attempted', 'three_pointers_made', 
            'three_pointers_attempted', 'free_throws_made', 'free_throws_attempted',
            'rebounds_offensive', 'rebounds_defensive', 'rebounds_total',
            'assists', 'steals', 'blocks', 'turnovers', 'fouls_personal', 'points'
        ]
        
        for field in numeric_fields:
            value = getattr(self, field, 0)
            if value is not None and value < 0:
                errors.append(f"{field} cannot be negative: {value}")
        
        return errors


class TeamGameTotal(Base):
    """
    Team-level game statistics and rankings table.
    
    Based on the totals_schema.json specification.
    Raw data imported directly from CSV files.
    """
    
    __tablename__ = 'teams_raw'
    
    # Primary key fields (composite key)
    game_id = Column('GAME_ID', BigInteger, nullable=False, comment="Unique identifier for each NBA game")
    team_id = Column('TEAM_ID', BigInteger, nullable=False, comment="Unique identifier for the NBA team")
    
    # Basic game information
    season_year = Column('SEASON_YEAR', String(7), nullable=False, comment="NBA season year")
    team_abbreviation = Column('TEAM_ABBREVIATION', String(3), nullable=False, comment="Three-letter team abbreviation")
    team_name = Column('TEAM_NAME', String(50), nullable=False, comment="Full official team name")
    game_date = Column('GAME_DATE', Date, nullable=False, comment="Date and time when game was played")
    matchup = Column('MATCHUP', String(20), nullable=False, comment="Team matchup")
    wl = Column('WL', String(1), nullable=False, comment="Game outcome: W for Win, L for Loss")
    
    # Game statistics
    min_played = Column('MIN', Float, nullable=False, comment="Total team minutes played")
    fgm = Column('FGM', Integer, nullable=False, comment="Field goals made")
    fga = Column('FGA', Integer, nullable=False, comment="Field goal attempts")
    fg_pct = Column('FG_PCT', Float, nullable=False, comment="Field goal percentage")
    
    fg3m = Column('FG3M', Integer, nullable=False, comment="Three-point field goals made")
    fg3a = Column('FG3A', Integer, nullable=False, comment="Three-point attempts")
    fg3_pct = Column('FG3_PCT', Float, nullable=False, comment="Three-point percentage")
    
    ftm = Column('FTM', Integer, nullable=False, comment="Free throws made")
    fta = Column('FTA', Integer, nullable=False, comment="Free throw attempts")
    ft_pct = Column('FT_PCT', Float, nullable=False, comment="Free throw percentage")
    
    oreb = Column('OREB', Integer, nullable=False, comment="Offensive rebounds")
    dreb = Column('DREB', Integer, nullable=False, comment="Defensive rebounds")
    reb = Column('REB', Integer, nullable=False, comment="Total rebounds")
    
    ast = Column('AST', Integer, nullable=False, comment="Assists")
    tov = Column('TOV', Float, nullable=False, comment="Turnovers")
    stl = Column('STL', Integer, nullable=False, comment="Steals")
    blk = Column('BLK', Integer, nullable=False, comment="Blocks")
    blka = Column('BLKA', Integer, nullable=False, comment="Opponent blocks")
    pf = Column('PF', Integer, nullable=False, comment="Personal fouls")
    pfd = Column('PFD', Integer, nullable=False, comment="Personal fouls drawn")
    pts = Column('PTS', Integer, nullable=False, comment="Total points scored")
    plus_minus = Column('PLUS_MINUS', Float, nullable=False, comment="Point differential")
    
    # Ranking fields (many ranking columns from schema)
    available_flag = Column('AVAILABLE_FLAG', Float, nullable=False, comment="Data availability flag")
    
    # Define composite primary key
    __table_args__ = (
        PrimaryKeyConstraint('GAME_ID', 'TEAM_ID', name='pk_teams_raw'),
        
        # Indexes for common queries
        Index('idx_teams_raw_team_date', 'TEAM_ID', 'GAME_DATE'),
        Index('idx_teams_raw_game', 'GAME_ID'),
        Index('idx_teams_raw_season', 'SEASON_YEAR'),
        Index('idx_teams_raw_team_season', 'TEAM_ID', 'SEASON_YEAR'),
        
        {
            'comment': 'Raw team game totals data imported from CSV files'
        }
    )
    
    def __repr__(self) -> str:
        """String representation of the model."""
        return (
            f"<TeamGameTotal(game_id={self.game_id}, team_id={self.team_id}, "
            f"team_name='{self.team_name}', pts={self.pts}, wl='{self.wl}')>"
        )
    
    @property
    def is_win(self) -> bool:
        """Check if team won the game."""
        return self.wl == 'W'
    
    @property
    def is_loss(self) -> bool:
        """Check if team lost the game."""
        return self.wl == 'L'


class PlayerProcessed(Base):
    """
    AI-optimized processed player data with advanced metrics.
    
    This table contains transformed player data with calculated advanced metrics,
    ready for AI agent analysis and reporting.
    """
    
    __tablename__ = 'players_processed'
    
    # Primary key fields
    game_id = Column('game_id', BigInteger, nullable=False, comment="Unique identifier for each NBA game")
    person_id = Column('person_id', Integer, nullable=False, comment="Unique identifier for the NBA player")
    
    # Basic game information
    season_year = Column('season_year', String(7), nullable=False, comment="NBA season year")
    game_date = Column('game_date', Date, nullable=False, comment="Date when the game was played")
    matchup = Column('matchup', String(20), nullable=True, comment="Team matchup")
    
    # Player and team information
    person_name = Column('person_name', String(100), nullable=False, comment="Full name of the player")
    team_id = Column('team_id', BigInteger, nullable=False, comment="Unique identifier for the NBA team")
    team_name = Column('team_name', String(50), nullable=False, comment="Official team name")
    team_tricode = Column('team_tricode', String(3), nullable=False, comment="Three-letter team abbreviation")
    position = Column('position', String(10), nullable=True, comment="Player's position")
    
    # Playing time
    minutes_played = Column('minutes_played', Float, nullable=False, default=0.0, comment="Minutes played in decimal format")
    is_dnp = Column('is_dnp', Boolean, nullable=False, default=False, comment="Did not play flag")
    
    # Basic box score stats
    points = Column('points', Integer, nullable=False, default=0, comment="Total points scored")
    field_goals_made = Column('field_goals_made', Integer, nullable=False, default=0, comment="Field goals made")
    field_goals_attempted = Column('field_goals_attempted', Integer, nullable=False, default=0, comment="Field goal attempts")
    three_pointers_made = Column('three_pointers_made', Integer, nullable=False, default=0, comment="Three-point shots made")
    three_pointers_attempted = Column('three_pointers_attempted', Integer, nullable=False, default=0, comment="Three-point attempts")
    free_throws_made = Column('free_throws_made', Integer, nullable=False, default=0, comment="Free throws made")
    free_throws_attempted = Column('free_throws_attempted', Integer, nullable=False, default=0, comment="Free throw attempts")
    rebounds_offensive = Column('rebounds_offensive', Integer, nullable=False, default=0, comment="Offensive rebounds")
    rebounds_defensive = Column('rebounds_defensive', Integer, nullable=False, default=0, comment="Defensive rebounds")
    rebounds_total = Column('rebounds_total', Integer, nullable=False, default=0, comment="Total rebounds")
    assists = Column('assists', Integer, nullable=False, default=0, comment="Assists")
    steals = Column('steals', Integer, nullable=False, default=0, comment="Steals")
    blocks = Column('blocks', Integer, nullable=False, default=0, comment="Blocked shots")
    turnovers = Column('turnovers', Integer, nullable=False, default=0, comment="Turnovers")
    fouls_personal = Column('fouls_personal', Integer, nullable=False, default=0, comment="Personal fouls")
    plus_minus = Column('plus_minus', Integer, nullable=False, default=0, comment="Plus-minus statistic")
    
    # Advanced shooting metrics
    true_shooting_percentage = Column('true_shooting_pct', Float, nullable=True, comment="True Shooting Percentage")
    effective_field_goal_percentage = Column('effective_fg_pct', Float, nullable=True, comment="Effective Field Goal Percentage")
    field_goal_percentage = Column('field_goal_pct', Float, nullable=True, comment="Field Goal Percentage")
    three_point_percentage = Column('three_point_pct', Float, nullable=True, comment="Three Point Percentage")
    free_throw_percentage = Column('free_throw_pct', Float, nullable=True, comment="Free Throw Percentage")
    
    # Advanced performance metrics
    player_efficiency_rating = Column('player_efficiency_rating', Float, nullable=True, comment="Player Efficiency Rating (simplified)")
    usage_rate = Column('usage_rate', Float, nullable=True, comment="Usage Rate estimation")
    defensive_impact_score = Column('defensive_impact_score', Float, nullable=True, comment="Defensive Impact Score (0-100)")
    
    # Per-36 minute stats
    points_per_36 = Column('points_per_36', Float, nullable=True, comment="Points per 36 minutes")
    rebounds_per_36 = Column('rebounds_per_36', Float, nullable=True, comment="Rebounds per 36 minutes")
    assists_per_36 = Column('assists_per_36', Float, nullable=True, comment="Assists per 36 minutes")
    steals_per_36 = Column('steals_per_36', Float, nullable=True, comment="Steals per 36 minutes")
    blocks_per_36 = Column('blocks_per_36', Float, nullable=True, comment="Blocks per 36 minutes")
    
    # Performance grades
    efficiency_grade = Column('efficiency_grade', String(2), nullable=True, comment="Shooting efficiency grade (A+ to D-)")
    defensive_grade = Column('defensive_grade', String(2), nullable=True, comment="Defensive performance grade (A+ to D-)")
    
    # Data processing metadata
    processed_at = Column('processed_at', Date, nullable=False, comment="Date when data was processed")
    source_validation_passed = Column('source_validation_passed', Boolean, nullable=False, default=True, comment="Source data validation status")
    
    # Define composite primary key
    __table_args__ = (
        PrimaryKeyConstraint('game_id', 'person_id', name='pk_players_processed'),
        
        # Indexes for analytics queries
        Index('idx_players_processed_person_date', 'person_id', 'game_date'),
        Index('idx_players_processed_person_season', 'person_id', 'season_year'),
        Index('idx_players_processed_team_date', 'team_id', 'game_date'),
        Index('idx_players_processed_season', 'season_year'),
        Index('idx_players_processed_efficiency', 'true_shooting_pct'),
        Index('idx_players_processed_per', 'player_efficiency_rating'),
        Index('idx_players_processed_minutes', 'minutes_played'),
        
        {
            'comment': 'AI-optimized processed player data with advanced basketball metrics'
        }
    )
    
    def __repr__(self) -> str:
        """String representation of the model."""
        return (
            f"<PlayerProcessed(game_id={self.game_id}, person_id={self.person_id}, "
            f"person_name='{self.person_name}', points={self.points}, "
            f"true_shooting_pct={self.true_shooting_percentage})>"
        )
    
    @property
    def is_starter(self) -> bool:
        """Estimate if player was a starter based on minutes played."""
        return self.minutes_played >= 20.0 and not self.is_dnp
    
    @property
    def is_significant_minutes(self) -> bool:
        """Check if player played significant minutes (>= 10 minutes)."""
        return self.minutes_played >= 10.0
    
    def get_per_minute_stats(self) -> Dict[str, Optional[float]]:
        """Calculate per-minute statistics."""
        if self.minutes_played <= 0:
            return {stat: None for stat in ['points_per_min', 'rebounds_per_min', 'assists_per_min']}
        
        return {
            'points_per_min': self.points / self.minutes_played,
            'rebounds_per_min': self.rebounds_total / self.minutes_played,
            'assists_per_min': self.assists / self.minutes_played,
            'steals_per_min': self.steals / self.minutes_played,
            'blocks_per_min': self.blocks / self.minutes_played
        }


class PlayerMonthlyTrend(Base):
    """
    Monthly aggregated player performance trends with recency weighting.
    
    This table stores monthly performance aggregations used for trend analysis
    and AI-powered insights.
    """
    
    __tablename__ = 'player_monthly_trends'
    
    # Primary key fields
    person_id = Column('person_id', Integer, nullable=False, comment="Unique identifier for the NBA player")
    season_year = Column('season_year', String(7), nullable=False, comment="NBA season year")
    month_year = Column('month_year', String(7), nullable=False, comment="Month-year in YYYY-MM format")
    
    # Basic information
    person_name = Column('person_name', String(100), nullable=False, comment="Full name of the player")
    games_played = Column('games_played', Integer, nullable=False, default=0, comment="Games played in the month")
    
    # Monthly averages - basic stats
    avg_minutes = Column('avg_minutes', Float, nullable=False, default=0.0, comment="Average minutes per game")
    avg_points = Column('avg_points', Float, nullable=False, default=0.0, comment="Average points per game")
    avg_rebounds = Column('avg_rebounds', Float, nullable=False, default=0.0, comment="Average rebounds per game")
    avg_assists = Column('avg_assists', Float, nullable=False, default=0.0, comment="Average assists per game")
    avg_steals = Column('avg_steals', Float, nullable=False, default=0.0, comment="Average steals per game")
    avg_blocks = Column('avg_blocks', Float, nullable=False, default=0.0, comment="Average blocks per game")
    avg_turnovers = Column('avg_turnovers', Float, nullable=False, default=0.0, comment="Average turnovers per game")
    
    # Monthly averages - shooting
    avg_field_goal_pct = Column('avg_field_goal_pct', Float, nullable=True, comment="Average field goal percentage")
    avg_three_point_pct = Column('avg_three_point_pct', Float, nullable=True, comment="Average three point percentage")
    avg_free_throw_pct = Column('avg_free_throw_pct', Float, nullable=True, comment="Average free throw percentage")
    avg_true_shooting_pct = Column('avg_true_shooting_pct', Float, nullable=True, comment="Average true shooting percentage")
    avg_effective_fg_pct = Column('avg_effective_fg_pct', Float, nullable=True, comment="Average effective field goal percentage")
    
    # Monthly averages - advanced metrics
    avg_player_efficiency_rating = Column('avg_per', Float, nullable=True, comment="Average Player Efficiency Rating")
    avg_usage_rate = Column('avg_usage_rate', Float, nullable=True, comment="Average Usage Rate")
    avg_defensive_impact_score = Column('avg_defensive_impact', Float, nullable=True, comment="Average Defensive Impact Score")
    
    # Trend analysis
    recency_weight = Column('recency_weight', Float, nullable=False, default=1.0, comment="Recency weighting factor")
    trend_direction = Column('trend_direction', String(20), nullable=True, comment="Trend direction (improving/declining/stable)")
    consistency_score = Column('consistency_score', Float, nullable=True, comment="Performance consistency score (0-100)")
    
    # Data processing metadata
    calculated_at = Column('calculated_at', Date, nullable=False, comment="Date when trends were calculated")
    
    # Define composite primary key
    __table_args__ = (
        PrimaryKeyConstraint('person_id', 'season_year', 'month_year', name='pk_player_monthly_trends'),
        
        # Indexes for trend queries
        Index('idx_player_trends_person', 'person_id'),
        Index('idx_player_trends_person_season', 'person_id', 'season_year'),
        Index('idx_player_trends_month', 'month_year'),
        Index('idx_player_trends_recency', 'recency_weight'),
        
        {
            'comment': 'Monthly aggregated player performance trends for AI analysis'
        }
    )
    
    def __repr__(self) -> str:
        """String representation of the model."""
        return (
            f"<PlayerMonthlyTrend(person_id={self.person_id}, month_year='{self.month_year}', "
            f"person_name='{self.person_name}', avg_points={self.avg_points})>"
        )