#!/usr/bin/env python3
"""
Player Monthly Trends Query Script

This script queries and displays player monthly trends from the database,
showing performance evolution over time with advanced analytics.

Usage:
    python scripts/query_player_trends.py "LeBron James"
    python scripts/query_player_trends.py "Stephen Curry" --season "2023-24"
    python scripts/query_player_trends.py --top-scorers --limit 10
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from analytics_pipeline.config.settings import Settings
from analytics_pipeline.config.database import DatabaseConfig
from analytics_pipeline.database.connection import DatabaseConnection
from analytics_pipeline.database.models import PlayerMonthlyTrend, PlayerProcessed


def format_percentage(value: Optional[float]) -> str:
    """Format percentage values for display."""
    if value is None:
        return "N/A"
    return f"{value:.1%}"


def format_decimal(value: Optional[float], decimals: int = 1) -> str:
    """Format decimal values for display."""
    if value is None:
        return "N/A"
    return f"{value:.{decimals}f}"


def format_trend_direction(direction: Optional[str]) -> str:
    """Format trend direction with emojis."""
    if direction == "improving":
        return "📈 Improving"
    elif direction == "declining":
        return "📉 Declining"
    elif direction == "stable":
        return "➡️ Stable"
    else:
        return "❓ Unknown"


def print_player_trends(
    db_connection: DatabaseConnection,
    player_name: str,
    season: Optional[str] = None,
    limit: int = 12
) -> bool:
    """
    Print monthly trends for a specific player.
    
    Args:
        db_connection: Database connection
        player_name: Player name to search for
        season: Optional season filter (e.g., '2023-24')
        limit: Maximum number of months to show
        
    Returns:
        True if player found, False otherwise
    """
    with db_connection.get_session() as session:
        # Build query
        query = session.query(PlayerMonthlyTrend).filter(
            PlayerMonthlyTrend.person_name.ilike(f'%{player_name}%')
        )
        
        if season:
            query = query.filter(PlayerMonthlyTrend.season_year == season)
        
        # Order by season and month (most recent first)
        trends = query.order_by(
            PlayerMonthlyTrend.season_year.desc(),
            PlayerMonthlyTrend.month_year.desc()
        ).limit(limit).all()
        
        if not trends:
            print(f"❌ No monthly trends found for '{player_name}'")
            if season:
                print(f"   in season {season}")
            return False
        
        # Get exact player name from first record
        exact_name = trends[0].person_name
        
        print(f"🏀 Monthly Trends for {exact_name}")
        print("=" * (len(exact_name) + 25))
        
        if season:
            print(f"📅 Season: {season}")
        else:
            seasons = set(trend.season_year for trend in trends)
            print(f"📅 Seasons: {', '.join(sorted(seasons, reverse=True))}")
        
        print(f"📊 Showing {len(trends)} most recent months\n")
        
        # Print header
        print(f"{'Month':<8} {'GP':<3} {'Min':<5} {'PTS':<5} {'REB':<5} {'AST':<5} "
              f"{'TS%':<6} {'PER':<6} {'DefImp':<7} {'Trend':<12}")
        print("-" * 85)
        
        # Print each month's data
        for trend in trends:
            month_display = trend.month_year
            trend_display = format_trend_direction(trend.trend_direction)
            
            print(f"{month_display:<8} "
                  f"{trend.games_played:<3} "
                  f"{format_decimal(trend.avg_minutes):<5} "
                  f"{format_decimal(trend.avg_points):<5} "
                  f"{format_decimal(trend.avg_rebounds):<5} "
                  f"{format_decimal(trend.avg_assists):<5} "
                  f"{format_percentage(trend.avg_true_shooting_pct):<6} "
                  f"{format_decimal(trend.avg_player_efficiency_rating):<6} "
                  f"{format_decimal(trend.avg_defensive_impact_score):<7} "
                  f"{trend_display:<12}")
        
        # Print summary statistics
        print("\n📈 Trend Summary:")
        
        # Calculate averages
        total_games = sum(t.games_played for t in trends)
        avg_points = sum(t.avg_points * t.games_played for t in trends) / total_games if total_games > 0 else 0
        avg_ts = sum((t.avg_true_shooting_pct or 0) * t.games_played for t in trends) / total_games if total_games > 0 else 0
        
        # Count trend directions
        trend_counts = {}
        for trend in trends:
            direction = trend.trend_direction or "unknown"
            trend_counts[direction] = trend_counts.get(direction, 0) + 1
        
        print(f"   • Total games: {total_games}")
        print(f"   • Average points: {avg_points:.1f}")
        print(f"   • Average TS%: {avg_ts:.1%}")
        
        if trend_counts:
            print(f"   • Trend breakdown:")
            for direction, count in sorted(trend_counts.items()):
                print(f"     - {format_trend_direction(direction)}: {count} months")
        
        return True


def print_top_performers(
    db_connection: DatabaseConnection,
    metric: str = "avg_points",
    season: Optional[str] = None,
    limit: int = 10,
    min_games: int = 5
) -> None:
    """
    Print top performers for a specific metric.
    
    Args:
        db_connection: Database connection
        metric: Metric to rank by (e.g., 'avg_points', 'avg_true_shooting_pct')
        season: Optional season filter
        limit: Number of players to show
        min_games: Minimum games per month required
    """
    metric_display_names = {
        'avg_points': 'Points Per Game',
        'avg_rebounds': 'Rebounds Per Game',
        'avg_assists': 'Assists Per Game',
        'avg_true_shooting_pct': 'True Shooting %',
        'avg_player_efficiency_rating': 'Player Efficiency Rating',
        'avg_defensive_impact_score': 'Defensive Impact Score'
    }
    
    display_name = metric_display_names.get(metric, metric.replace('_', ' ').title())
    
    with db_connection.get_session() as session:
        # Build query - get average performance across all months
        query = session.query(
            PlayerMonthlyTrend.person_name,
            PlayerMonthlyTrend.season_year,
            session.query(session.func.sum(PlayerMonthlyTrend.games_played)).filter(
                PlayerMonthlyTrend.person_name == PlayerMonthlyTrend.person_name
            ).label('total_games'),
            session.query(session.func.avg(getattr(PlayerMonthlyTrend, metric))).filter(
                PlayerMonthlyTrend.person_name == PlayerMonthlyTrend.person_name,
                PlayerMonthlyTrend.games_played >= min_games
            ).label('avg_metric')
        )
        
        # Filter by season if specified
        if season:
            query = query.filter(PlayerMonthlyTrend.season_year == season)
        
        # Group by player and season, filter by minimum games
        results = query.group_by(
            PlayerMonthlyTrend.person_name,
            PlayerMonthlyTrend.season_year
        ).having(
            session.func.sum(PlayerMonthlyTrend.games_played) >= min_games * 3  # At least 3 months of data
        ).order_by(
            getattr(PlayerMonthlyTrend, metric).desc()
        ).limit(limit).all()
        
        if not results:
            print(f"❌ No data found for {display_name}")
            return
        
        print(f"🏆 Top {limit} Players by {display_name}")
        if season:
            print(f"📅 Season: {season}")
        else:
            print("📅 All seasons")
        print("=" * 60)
        
        print(f"{'Rank':<4} {'Player':<25} {'Season':<8} {'Value':<10} {'Games':<6}")
        print("-" * 60)
        
        for i, result in enumerate(results, 1):
            if hasattr(result, 'person_name'):
                name = result.person_name
                season_yr = result.season_year
                value = getattr(result, metric, None)
                games = result.total_games
            else:
                # Handle tuple results
                name, season_yr, games, value = result
            
            # Format value based on metric type
            if 'pct' in metric:
                value_display = format_percentage(value)
            else:
                value_display = format_decimal(value, 1)
            
            print(f"{i:<4} {name:<25} {season_yr:<8} {value_display:<10} {games:<6}")


def search_players(db_connection: DatabaseConnection, search_term: str) -> List[str]:
    """Search for players matching the search term."""
    with db_connection.get_session() as session:
        players = session.query(PlayerMonthlyTrend.person_name).filter(
            PlayerMonthlyTrend.person_name.ilike(f'%{search_term}%')
        ).distinct().limit(10).all()
        
        return [player.person_name for player in players]


def print_available_seasons(db_connection: DatabaseConnection) -> None:
    """Print available seasons in the database."""
    with db_connection.get_session() as session:
        seasons = session.query(PlayerMonthlyTrend.season_year).distinct().order_by(
            PlayerMonthlyTrend.season_year.desc()
        ).all()
        
        print("📅 Available Seasons:")
        for season in seasons:
            print(f"   • {season.season_year}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Query and display NBA player monthly trends"
    )
    
    # Player-specific queries
    parser.add_argument(
        "player_name",
        nargs="?",
        help="Player name to search for (e.g., 'LeBron James')"
    )
    
    parser.add_argument(
        "--season",
        help="Filter by specific season (e.g., '2023-24')"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=12,
        help="Maximum number of months/players to show (default: 12)"
    )
    
    # Top performers queries
    parser.add_argument(
        "--top-scorers",
        action="store_true",
        help="Show top scorers"
    )
    
    parser.add_argument(
        "--top-rebounds",
        action="store_true",
        help="Show top rebounders"
    )
    
    parser.add_argument(
        "--top-assists",
        action="store_true",
        help="Show top assist leaders"
    )
    
    parser.add_argument(
        "--top-efficiency",
        action="store_true",
        help="Show top players by True Shooting %"
    )
    
    parser.add_argument(
        "--top-defense",
        action="store_true",
        help="Show top defensive players"
    )
    
    # Utility options
    parser.add_argument(
        "--search",
        help="Search for players by name"
    )
    
    parser.add_argument(
        "--list-seasons",
        action="store_true",
        help="List available seasons"
    )
    
    args = parser.parse_args()
    
    try:
        # Setup database connection
        settings = Settings()
        db_config = DatabaseConfig(settings)
        db_connection = DatabaseConnection(db_config)
        
        # Test connection
        if not db_connection.test_connection():
            print("❌ Database connection failed!")
            print("💡 Make sure PostgreSQL is running and your .env file is configured correctly")
            return False
        
        print("🏀 NBA Player Monthly Trends Query Tool")
        print("=" * 45)
        
        # Handle different query types
        if args.list_seasons:
            print_available_seasons(db_connection)
            
        elif args.search:
            players = search_players(db_connection, args.search)
            if players:
                print(f"🔍 Players matching '{args.search}':")
                for player in players:
                    print(f"   • {player}")
            else:
                print(f"❌ No players found matching '{args.search}'")
        
        elif args.top_scorers:
            print_top_performers(db_connection, "avg_points", args.season, args.limit)
        
        elif args.top_rebounds:
            print_top_performers(db_connection, "avg_rebounds", args.season, args.limit)
        
        elif args.top_assists:
            print_top_performers(db_connection, "avg_assists", args.season, args.limit)
        
        elif args.top_efficiency:
            print_top_performers(db_connection, "avg_true_shooting_pct", args.season, args.limit)
        
        elif args.top_defense:
            print_top_performers(db_connection, "avg_defensive_impact_score", args.season, args.limit)
        
        elif args.player_name:
            success = print_player_trends(db_connection, args.player_name, args.season, args.limit)
            if not success:
                # Try to suggest similar players
                similar_players = search_players(db_connection, args.player_name)
                if similar_players:
                    print(f"\n💡 Did you mean one of these players?")
                    for player in similar_players[:5]:
                        print(f"   • {player}")
        
        else:
            # Show usage examples
            print("📖 Usage Examples:")
            print("   python scripts/query_player_trends.py 'LeBron James'")
            print("   python scripts/query_player_trends.py 'Stephen Curry' --season '2023-24'")
            print("   python scripts/query_player_trends.py --top-scorers --limit 10")
            print("   python scripts/query_player_trends.py --search 'Durant'")
            print("   python scripts/query_player_trends.py --list-seasons")
        
        # Close connection
        db_connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)