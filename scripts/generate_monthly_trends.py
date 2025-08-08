#!/usr/bin/env python3
"""
Generate Monthly Trends Script

This script generates monthly aggregated trends from processed player data
and populates the player_monthly_trends table for trend analysis.

Usage:
    python scripts/generate_monthly_trends.py
    python scripts/generate_monthly_trends.py --season "2023-24"
    python scripts/generate_monthly_trends.py --player "LeBron James"
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from collections import defaultdict
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from nba_analyst.config.settings import Settings
from nba_analyst.config.database import DatabaseConfig
from nba_analyst.database.connection import DatabaseConnection
from nba_analyst.database.models import PlayerProcessed, PlayerMonthlyTrend
from nba_analyst.analytics.trends import TrendAnalyzer


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


class MonthlyTrendGenerator:
    """Generates monthly trends from processed player data."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """Initialize the monthly trend generator."""
        self.db_connection = db_connection
        self.logger = logging.getLogger(__name__)
    
    def generate_trends_for_player(
        self, 
        person_id: int, 
        person_name: str,
        season_year: Optional[str] = None
    ) -> int:
        """
        Generate monthly trends for a specific player.
        
        Args:
            person_id: Player's unique ID
            person_name: Player's name
            season_year: Optional season filter
            
        Returns:
            Number of monthly trend records created
        """
        with self.db_connection.get_session() as session:
            # Query processed data for the player
            query = session.query(PlayerProcessed).filter(
                PlayerProcessed.person_id == person_id
            )
            
            if season_year:
                query = query.filter(PlayerProcessed.season_year == season_year)
            
            games = query.order_by(PlayerProcessed.game_date).all()
            
            if not games:
                return 0
            
            # Group games by month
            monthly_games = defaultdict(list)
            
            for game in games:
                # Create month-year key (YYYY-MM format)
                month_key = game.game_date.strftime('%Y-%m')
                monthly_games[month_key].append(game)
            
            trends_created = 0
            
            # Generate trends for each month
            for month_key, month_games in monthly_games.items():
                if len(month_games) < 2:  # Need at least 2 games for meaningful trends
                    continue
                
                # Calculate monthly averages
                monthly_trend = self._calculate_monthly_averages(
                    person_id, person_name, month_key, month_games
                )
                
                # Check if trend already exists
                existing_trend = session.query(PlayerMonthlyTrend).filter(
                    PlayerMonthlyTrend.person_id == person_id,
                    PlayerMonthlyTrend.season_year == month_games[0].season_year,
                    PlayerMonthlyTrend.month_year == month_key
                ).first()
                
                if existing_trend:
                    # Update existing trend
                    for attr, value in monthly_trend.items():
                        setattr(existing_trend, attr, value)
                    self.logger.debug(f"Updated trend for {person_name} {month_key}")
                else:
                    # Create new trend
                    trend_record = PlayerMonthlyTrend(**monthly_trend)
                    session.add(trend_record)
                    trends_created += 1
                    self.logger.debug(f"Created trend for {person_name} {month_key}")
            
            session.commit()
            return trends_created
    
    def _calculate_monthly_averages(
        self,
        person_id: int,
        person_name: str,
        month_key: str,
        games: List[PlayerProcessed]
    ) -> Dict[str, Any]:
        """Calculate monthly averages from games."""
        
        if not games:
            return {}
        
        # Filter out DNP games for meaningful averages
        active_games = [g for g in games if not g.is_dnp and g.minutes_played > 0]
        
        if not active_games:
            # If all games were DNP, use the original list but with zeros
            active_games = games
        
        num_games = len(games)
        num_active_games = len(active_games) 
        
        # Calculate averages
        def safe_avg(values):
            """Calculate average, handling None values."""
            valid_values = [v for v in values if v is not None]
            return sum(valid_values) / len(valid_values) if valid_values else 0.0
        
        # Basic stats averages
        avg_minutes = safe_avg([g.minutes_played for g in active_games])
        avg_points = safe_avg([g.points for g in active_games])
        avg_rebounds = safe_avg([g.rebounds_total for g in active_games])
        avg_assists = safe_avg([g.assists for g in active_games])
        avg_steals = safe_avg([g.steals for g in active_games])
        avg_blocks = safe_avg([g.blocks for g in active_games])
        avg_turnovers = safe_avg([g.turnovers for g in active_games])
        
        # Shooting percentages (only from games with attempts)
        fg_games = [g for g in active_games if g.field_goals_attempted > 0]
        three_games = [g for g in active_games if g.three_pointers_attempted > 0]
        ft_games = [g for g in active_games if g.free_throws_attempted > 0]
        
        avg_field_goal_pct = safe_avg([g.field_goal_percentage for g in fg_games]) if fg_games else None
        avg_three_point_pct = safe_avg([g.three_point_percentage for g in three_games]) if three_games else None
        avg_free_throw_pct = safe_avg([g.free_throw_percentage for g in ft_games]) if ft_games else None
        
        # Advanced metrics
        avg_true_shooting_pct = safe_avg([g.true_shooting_percentage for g in active_games if g.true_shooting_percentage is not None])
        avg_effective_fg_pct = safe_avg([g.effective_field_goal_percentage for g in active_games if g.effective_field_goal_percentage is not None])
        avg_per = safe_avg([g.player_efficiency_rating for g in active_games if g.player_efficiency_rating is not None])
        avg_usage_rate = safe_avg([g.usage_rate for g in active_games if g.usage_rate is not None])
        avg_def_impact = safe_avg([g.defensive_impact_score for g in active_games if g.defensive_impact_score is not None])
        
        # Simple trend detection (comparing first half vs second half of month)
        if len(active_games) >= 4:
            mid_point = len(active_games) // 2
            first_half_pts = safe_avg([g.points for g in active_games[:mid_point]])
            second_half_pts = safe_avg([g.points for g in active_games[mid_point:]])
            
            if second_half_pts > first_half_pts * 1.05:  # 5% improvement
                trend_direction = "improving"
            elif second_half_pts < first_half_pts * 0.95:  # 5% decline
                trend_direction = "declining"
            else:
                trend_direction = "stable"
        else:
            trend_direction = "stable"
        
        # Calculate consistency score (inverse of coefficient of variation)
        points_values = [g.points for g in active_games]
        if len(points_values) > 1:
            points_std = (sum((x - avg_points) ** 2 for x in points_values) / len(points_values)) ** 0.5
            cv = points_std / avg_points if avg_points > 0 else 0
            consistency_score = max(100 - (cv * 100), 0)  # Convert to 0-100 scale
        else:
            consistency_score = 100.0
        
        return {
            'person_id': person_id,
            'season_year': games[0].season_year,
            'month_year': month_key,
            'person_name': person_name,
            'games_played': num_games,
            
            # Basic stats
            'avg_minutes': avg_minutes,
            'avg_points': avg_points,
            'avg_rebounds': avg_rebounds,
            'avg_assists': avg_assists,
            'avg_steals': avg_steals,
            'avg_blocks': avg_blocks,
            'avg_turnovers': avg_turnovers,
            
            # Shooting percentages
            'avg_field_goal_pct': avg_field_goal_pct,
            'avg_three_point_pct': avg_three_point_pct,
            'avg_free_throw_pct': avg_free_throw_pct,
            'avg_true_shooting_pct': avg_true_shooting_pct,
            'avg_effective_fg_pct': avg_effective_fg_pct,
            
            # Advanced metrics
            'avg_player_efficiency_rating': avg_per,
            'avg_usage_rate': avg_usage_rate,
            'avg_defensive_impact_score': avg_def_impact,
            
            # Trend analysis
            'recency_weight': 1.0,  # Default weight
            'trend_direction': trend_direction,
            'consistency_score': consistency_score,
            
            # Metadata
            'calculated_at': date.today()
        }
    
    def generate_all_trends(self, season_year: Optional[str] = None) -> Dict[str, int]:
        """
        Generate monthly trends for all players.
        
        Args:
            season_year: Optional season filter
            
        Returns:
            Dictionary with generation statistics
        """
        self.logger.info("Starting monthly trends generation...")
        
        with self.db_connection.get_session() as session:
            # Get unique players
            query = session.query(
                PlayerProcessed.person_id,
                PlayerProcessed.person_name
            ).distinct()
            
            if season_year:
                query = query.filter(PlayerProcessed.season_year == season_year)
            
            players = query.all()
            
            self.logger.info(f"Found {len(players)} unique players to process")
            
            total_trends = 0
            processed_players = 0
            
            for person_id, person_name in players:
                try:
                    trends_created = self.generate_trends_for_player(
                        person_id, person_name, season_year
                    )
                    total_trends += trends_created
                    processed_players += 1
                    
                    if processed_players % 50 == 0:
                        self.logger.info(f"Processed {processed_players}/{len(players)} players...")
                
                except Exception as e:
                    self.logger.error(f"Error processing {person_name}: {str(e)}")
                    continue
        
        self.logger.info(f"Monthly trends generation complete!")
        
        return {
            'players_processed': processed_players,
            'total_players': len(players),
            'trends_created': total_trends,
            'success_rate': processed_players / len(players) if players else 0.0
        }


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Generate monthly trends from processed player data"
    )
    
    parser.add_argument(
        "--season", 
        help="Generate trends for specific season (e.g., '2023-24')"
    )
    
    parser.add_argument(
        "--player",
        help="Generate trends for specific player only"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger = logging.getLogger(__name__)
    
    try:
        # Setup database connection
        settings = Settings()
        db_config = DatabaseConfig(settings)
        db_connection = DatabaseConnection(db_config)
        
        # Test connection
        if not db_connection.test_connection():
            print("❌ Database connection failed!")
            return False
        
        print("📊 NBA Monthly Trends Generation")
        print("=" * 35)
        
        # Initialize generator
        generator = MonthlyTrendGenerator(db_connection)
        
        start_time = datetime.now()
        
        if args.player:
            # Generate trends for specific player
            with db_connection.get_session() as session:
                # Find player
                player = session.query(PlayerProcessed).filter(
                    PlayerProcessed.person_name.ilike(f'%{args.player}%')
                ).first()
                
                if not player:
                    print(f"❌ Player '{args.player}' not found")
                    return False
                
                print(f"🏀 Generating trends for {player.person_name}")
                
                trends_created = generator.generate_trends_for_player(
                    player.person_id, player.person_name, args.season
                )
                
                print(f"✅ Created {trends_created} monthly trend records")
        
        else:
            # Generate trends for all players
            if args.season:
                print(f"📅 Processing season: {args.season}")
            else:
                print("📅 Processing all seasons")
            
            results = generator.generate_all_trends(args.season)
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            print(f"\n📈 Generation Results:")
            print(f"   • Players processed: {results['players_processed']}/{results['total_players']}")
            print(f"   • Monthly trends created: {results['trends_created']}")
            print(f"   • Success rate: {results['success_rate']:.1%}")
            print(f"   • Duration: {duration}")
        
        # Close connection
        db_connection.close()
        
        print(f"\n✅ Monthly trends generation completed!")
        print(f"🔍 Query trends with: python scripts/query_player_trends.py")
        
        return True
        
    except Exception as e:
        logger.error(f"Trends generation failed: {str(e)}")
        print(f"❌ Error: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)