"""
SQLAlchemy models for NBA Analyst Agent database.

This module defines the database schema using SQLAlchemy ORM models
based on the JSON schema specifications for NBA data.
"""

from datetime import date
from typing import Optional

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
            self.minutes in ("0", "0:00", None) or
            (self.comment and "DNP" in self.comment)
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