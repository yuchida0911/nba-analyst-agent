from __future__ import annotations

"""
NBA Analytics tools for Google ADK.

This module provides a comprehensive suite of basketball analytics tools that enable
AI agents to perform sophisticated NBA data analysis. The tools are organized into
several categories:

BASIC DATA RETRIEVAL:
- get_player_stats: Individual player game logs and statistics
- get_player_stats_by_season: Season-by-season player averages
- get_team_stats: Team-level game statistics and performance
- get_player_monthly_trends: Month-by-month performance trends

ADVANCED PLAYER ANALYSIS:
- analyze_player_efficiency: Comprehensive shooting and scoring efficiency
- analyze_player_defense: Defensive impact and contribution analysis
- compare_players_advanced_metrics: Side-by-side player comparisons
- predict_player_performance: Performance prediction based on trends
- calculate_advanced_basketball_metrics: Advanced metrics (PER, TS%, etc.)

TEAM ANALYSIS:
- analyze_team_performance_trends: Team performance over time
- analyze_lineup_effectiveness: Team roster and lineup analysis

PLAYER-TEAM COMBINED ANALYSIS:
- analyze_player_team_impact: Player's contribution to team performance
- analyze_player_team_synergy: Player-team fit and compatibility
- compare_teams_player_impact: How different teams utilize players
- analyze_team_offensive_efficiency_by_player_contribution: Individual impact on team efficiency

STATISTICAL ANALYSIS:
- analyze_statistical_correlations: Statistical relationships in player data
- cluster_players_by_playing_style: Group players by statistical similarity
- analyze_player_performance_by_game_situation: Situational performance analysis

CUSTOM QUERIES:
- run_query: Execute custom BigQuery SQL
- get_query_status: Check query execution status
- get_query_results: Retrieve custom query results

All tools return JSON-serializable dictionaries with a `status` field indicating
success or error, along with relevant data and analysis results.
"""

from dataclasses import asdict
from datetime import date, datetime
import os
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery
from google.api_core import exceptions as gcloud_exceptions

from google.adk.tools import FunctionTool

from nba_analyst.analytics.metrics import PlayerGameStats
from nba_analyst.analytics.efficiency import EfficiencyAnalyzer
from nba_analyst.analytics.defensive import (
    analyze_defensive_strengths,
)


DEFAULT_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "yuchida-dev")
DATASET_ID = os.getenv("NBA_ANALYTICS_DATASET", "nba_analytics")
RAW_TABLE = f"{DATASET_ID}.players_raw"
TEAM_STATS_TABLE = f"{DATASET_ID}.totals"


def _bq_client(project_id: Optional[str] = None) -> bigquery.Client:
    return bigquery.Client(project=project_id or DEFAULT_PROJECT_ID)


def _parse_minutes_str_to_decimal(minutes_str: Optional[str]) -> float:
    if not minutes_str:
        return 0.0
    if minutes_str == "0":
        return 0.0
    try:
        parts = minutes_str.split(":")
        if len(parts) == 2:
            mins = int(parts[0])
            secs = int(parts[1])
            return mins + secs / 60.0
        # Fallback if already numeric-string
        return float(minutes_str)
    except Exception:
        return 0.0


def _row_to_player_game_stats(row: bigquery.table.Row) -> PlayerGameStats:
    return PlayerGameStats(
        points=int(row.get("points") or 0),
        field_goals_made=int(row.get("fieldGoalsMade") or 0),
        field_goals_attempted=int(row.get("fieldGoalsAttempted") or 0),
        three_pointers_made=int(row.get("threePointersMade") or 0),
        three_pointers_attempted=int(row.get("threePointersAttempted") or 0),
        free_throws_made=int(row.get("freeThrowsMade") or 0),
        free_throws_attempted=int(row.get("freeThrowsAttempted") or 0),
        rebounds_offensive=int(row.get("reboundsOffensive") or 0),
        rebounds_defensive=int(row.get("reboundsDefensive") or 0),
        rebounds_total=int(row.get("reboundsTotal") or 0),
        assists=int(row.get("assists") or 0),
        steals=int(row.get("steals") or 0),
        blocks=int(row.get("blocks") or 0),
        turnovers=int(row.get("turnovers") or 0),
        fouls_personal=int(row.get("foulsPersonal") or 0),
        minutes_played=_parse_minutes_str_to_decimal(row.get("minutes")),
    )


def run_query(query: str, project_id: Optional[str] = None) -> Dict[str, Any]:
    """Execute a BigQuery SQL query asynchronously.

    Use this tool when you need to run custom SQL queries against the NBA dataset.
    This is useful for complex analysis that isn't covered by other specialized tools.
    
    The query runs asynchronously - use get_query_status and get_query_results 
    to check completion and retrieve results.

    Args:
        query: BigQuery SQL query string. Available tables:
               - `nba_analytics.players_raw`: Individual player game statistics
               - `nba_analytics.totals`: Team-level game statistics
        project_id: Optional GCP project ID. Defaults to env/default.

    Returns:
        dict with keys:
        - status: "success" or "error"
        - job_id: BigQuery job ID for tracking
        - project_id: GCP project used
        - location: BigQuery job location
        - message: Error message if status is "error"
        
    Example Usage:
        Use for custom analysis like "Find games where a team scored over 140 points"
        or "Calculate league-wide shooting percentages by month".
    """
    try:
        client = _bq_client(project_id)
        job = client.query(query)
        return {
            "status": "success",
            "job_id": job.job_id,
            "project_id": client.project,
            "location": job.location,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_query_status(job_id: str, project_id: Optional[str] = None, location: Optional[str] = None) -> Dict[str, Any]:
    """Check the execution status of a BigQuery query job.

    Use this tool after running a query with run_query to check if it has completed.
    This is essential for asynchronous query execution workflow.

    Args:
        job_id: BigQuery job ID returned from run_query
        project_id: Optional GCP project ID
        location: Optional BigQuery job location

    Returns:
        dict with keys:
        - status: "success" or "error"
        - job_id: The query job ID
        - state: Job state ("PENDING", "RUNNING", "DONE")
        - done: Boolean indicating if job is complete
        - error_result: Error details if job failed
        - message: Error message if status is "error"
        
    Example Usage:
        Check if a long-running analysis query has finished before retrieving results.
    """
    try:
        client = _bq_client(project_id)
        job = client.get_job(job_id, location=location)
        return {
            "status": "success",
            "job_id": job.job_id,
            "state": job.state,
            "done": job.done(),
            "error_result": job.error_result,
        }
    except gcloud_exceptions.NotFound:
        return {"status": "error", "message": f"Job {job_id} not found"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_query_results(job_id: str, project_id: Optional[str] = None, location: Optional[str] = None, max_rows: int = 1000) -> Dict[str, Any]:
    """Retrieve the results of a completed BigQuery query job.

    Use this tool to get the actual data from a query after confirming it's done
    with get_query_status. This completes the asynchronous query workflow.

    Args:
        job_id: BigQuery job ID from run_query
        project_id: Optional GCP project ID
        location: Optional BigQuery job location
        max_rows: Maximum number of rows to return (default: 1000)

    Returns:
        dict with keys:
        - status: "success" or "error"
        - job_id: The query job ID
        - row_count: Number of rows returned
        - records: List of dictionaries containing query results
        - message: Error message if status is "error"
        
    Example Usage:
        Get results after a custom analysis query completes successfully.
    """
    try:
        client = _bq_client(project_id)
        job = client.get_job(job_id, location=location)
        rows = list(job.result(page_size=max_rows))
        # Convert Row objects to dict
        data: List[Dict[str, Any]] = []
        if rows:
            if job.destination and getattr(job.destination, "schema", None):
                fields = [schema_field.name for schema_field in job.destination.schema]
                data = [
                    {name: row.get(name) for name in fields}
                    for row in rows[:max_rows]
                ]
            else:
                sample = rows[0]
                # Fallback to keys on Row
                try:
                    keys = list(sample.keys())
                except Exception:
                    keys = []
                for row in rows[:max_rows]:
                    if keys:
                        data.append({k: row.get(k) for k in keys})
                    else:
                        data.append(dict(row))
        return {
            "status": "success",
            "job_id": job.job_id,
            "row_count": len(data),
            "records": data,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_player_stats(player_name: str, season_year: Optional[str] = None, limit: int = 50, project_id: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve individual game statistics for a specific player.

    This is the primary tool for getting detailed game-by-game performance data
    for any NBA player. Use this when you need to analyze recent performance,
    game logs, or individual game statistics.

    Args:
        player_name: Player name (case-insensitive, partial matches work)
                    Examples: "LeBron James", "LeBron", "James"
        season_year: Optional season filter in format "YYYY-YY" 
                    Examples: "2023-24", "2022-23"
        limit: Maximum number of games to return (default: 50, most recent first)
        project_id: Optional GCP project ID

    Returns:
        dict with keys:
        - status: "success" or "error"
        - player: Player name searched
        - season_year: Season filter applied (if any)
        - count: Number of games returned
        - records: List of game statistics including:
          * game_date, season_year, personName, teamTricode
          * minutes, points, rebounds, assists, steals, blocks
          * field goals, three-pointers, free throws (made/attempted)
          * turnovers, fouls, plus/minus
        - message: Error message if status is "error"

    Example Usage:
        - "Show me LeBron's last 10 games"
        - "Get Stephen Curry's 2023-24 season games"
        - "What did Giannis score in his recent games?"
    """
    try:
        client = _bq_client(project_id)
        name_esc = player_name.replace("'", "\'")
        season_filter = f"AND season_year = '{season_year}'" if season_year else ""
        query = f"""
        SELECT
          game_date,
          season_year,
          personId,
          personName,
          teamId,
          teamTricode,
          minutes,
          fieldGoalsMade, fieldGoalsAttempted, threePointersMade, threePointersAttempted,
          freeThrowsMade, freeThrowsAttempted,
          reboundsOffensive, reboundsDefensive, reboundsTotal,
          assists, steals, blocks, turnovers, foulsPersonal, points,
          plusMinusPoints
        FROM `{client.project}.{RAW_TABLE}`
        WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
        {season_filter}
        ORDER BY game_date DESC
        LIMIT {int(limit)}
        """
        rows = list(client.query(query).result())
        records = [dict(row) for row in rows]
        return {
            "status": "success",
            "player": player_name,
            "season_year": season_year,
            "count": len(records),
            "records": records,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_player_stats_by_season(player_name: str, project_id: Optional[str] = None) -> Dict[str, Any]:
    """Get season-by-season statistical averages for a player.

    This tool provides aggregated season statistics rather than individual games.
    Use this when you want to see a player's career progression, compare seasons,
    or get overall performance metrics across multiple years.

    Args:
        player_name: Player name (case-insensitive, partial matches work)
        project_id: Optional GCP project ID

    Returns:
        dict with keys:
        - status: "success" or "error"
        - player: Player name searched
        - by_season: List of season statistics including:
          * season_year, games_played
          * avg_points, avg_rebounds, avg_assists, avg_steals, avg_blocks
          * avg_fg_pct, avg_3p_pct, avg_ft_pct, avg_ts_pct (True Shooting %)
          * avg_turnovers
        - message: Error message if status is "error"

    Example Usage:
        - "Show me LeBron's career averages by season"
        - "How has Curry's three-point shooting changed over the years?"
        - "Compare Giannis's rookie season to his MVP seasons"
    """
    try:
        client = _bq_client(project_id)
        name_esc = player_name.replace("'", "\'")
        query = f"""
        SELECT
          season_year,
          COUNT(1) AS games_played,
          AVG(points) AS avg_points,
          AVG(reboundsTotal) AS avg_rebounds,
          AVG(assists) AS avg_assists,
          AVG(steals) AS avg_steals,
          AVG(blocks) AS avg_blocks,
          AVG(turnovers) AS avg_turnovers,
          AVG(IF(fieldGoalsAttempted>0, fieldGoalsMade/fieldGoalsAttempted, NULL)) AS avg_fg_pct,
          AVG(IF(threePointersAttempted>0, threePointersMade/threePointersAttempted, NULL)) AS avg_3p_pct,
          AVG(IF(freeThrowsAttempted>0, freeThrowsMade/freeThrowsAttempted, NULL)) AS avg_ft_pct,
          AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct
        FROM `{client.project}.{RAW_TABLE}`
        WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
        GROUP BY season_year
        ORDER BY season_year DESC
        """
        rows = list(client.query(query).result())
        return {
            "status": "success",
            "player": player_name,
            "by_season": [dict(row) for row in rows],
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_team_stats(team_identifier: str, season_year: Optional[str] = None, limit: int = 50, project_id: Optional[str] = None) -> Dict[str, Any]:
    """Get team-level game statistics and performance data.

    This tool retrieves team aggregated statistics per game from the totals table.
    Use this when you need team performance data, game results, or team-level
    statistical analysis rather than individual player stats.

    Args:
        team_identifier: Team identifier in one of these formats:
                        - Team ID (numeric): "1610612744"
                        - Team abbreviation (3 letters): "GSW", "LAL", "BOS"
                        - Team name: "Golden State Warriors", "Lakers"
        season_year: Optional season filter in format "YYYY-YY"
        limit: Maximum number of games to return (default: 50, most recent first)
        project_id: Optional GCP project ID

    Returns:
        dict with keys:
        - status: "success" or "error"
        - team: Team identifier searched
        - season_year: Season filter applied (if any)
        - count: Number of games returned
        - records: List of team game statistics including:
          * GAME_DATE, SEASON_YEAR, TEAM_ID, teamTricode
          * points, rebounds, assists, steals, blocks, turnovers
          * fgm, fga, tpm, tpa, ftm, fta (field goals, three-pointers, free throws)
          * efg_pct (Effective Field Goal Percentage)
        - message: Error message if status is "error"

    Example Usage:
        - "Show me the Lakers' recent game results"
        - "Get Golden State Warriors' 2023-24 season stats"
        - "How many points did the Celtics average this season?"
    """
    try:
        client = _bq_client(project_id)
        # Build identifier predicate
        pred: str
        if team_identifier.isdigit():
            pred = f"TEAM_ID = {int(team_identifier)}"
        elif len(team_identifier) == 3:
            pred = f"UPPER(TEAM_ABBREVIATION) = UPPER('{team_identifier}')"
        else:
            pred = f"LOWER(TEAM_NAME) = LOWER('{team_identifier}')"

        season_filter = f"AND SEASON_YEAR = '{season_year}'" if season_year else ""
        query = f"""
        SELECT
          GAME_DATE,
          SEASON_YEAR,
          TEAM_ID,
          ANY_VALUE(TEAM_ABBREVIATION) AS teamTricode,
          SUM(PTS) AS points,
          SUM(REB) AS rebounds,
          SUM(AST) AS assists,
          SUM(STL) AS steals,
          SUM(BLK) AS blocks,
          SUM(TOV) AS turnovers,
          SUM(FGM) AS fgm,
          SUM(FGA) AS fga,
          SUM(FG3M) AS tpm,
          SUM(FG3A) AS tpa,
          SUM(FTM) AS ftm,
          SUM(FTA) AS fta,
          SAFE_DIVIDE(SUM(FGM) + 0.5*SUM(FG3M), NULLIF(SUM(FGA),0)) AS efg_pct
        FROM `{client.project}.{TEAM_STATS_TABLE}`
        WHERE {pred}
        {season_filter}
        GROUP BY GAME_DATE, SEASON_YEAR, TEAM_ID
        ORDER BY GAME_DATE DESC
        LIMIT {int(limit)}
        """
        rows = list(client.query(query).result())
        return {
            "status": "success",
            "team": team_identifier,
            "season_year": season_year,
            "count": len(rows),
            "records": [dict(row) for row in rows],
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_player_monthly_trends(player_name: str, project_id: Optional[str] = None, limit_months: int = 12) -> Dict[str, Any]:
    """Analyze player performance trends by month over time.

    This tool shows how a player's performance changes month by month, revealing
    seasonal patterns, hot streaks, slumps, and overall trends. Perfect for
    identifying when players perform best during the season.

    Args:
        player_name: Player name (case-insensitive, partial matches work)
        project_id: Optional GCP project ID
        limit_months: Maximum number of months to return (default: 12, most recent first)

    Returns:
        dict with keys:
        - status: "success" or "error"
        - player: Player name searched
        - months: List of monthly statistics including:
          * month_year (format: "YYYY-MM"), season_year, games_played
          * avg_points, avg_rebounds, avg_assists
          * avg_fg_pct, avg_3p_pct, avg_ts_pct (True Shooting %)
        - message: Error message if status is "error"

    Example Usage:
        - "How has Curry performed each month this season?"
        - "Show me LeBron's monthly trends over the last year"
        - "When does Giannis typically play his best basketball?"
    """
    try:
        client = _bq_client(project_id)
        name_esc = player_name.replace("'", "\'")
        query = f"""
        SELECT
          FORMAT_DATE('%Y-%m', game_date) AS month_year,
          season_year,
          COUNT(1) AS games_played,
          AVG(points) AS avg_points,
          AVG(reboundsTotal) AS avg_rebounds,
          AVG(assists) AS avg_assists,
          AVG(IF(fieldGoalsAttempted>0, fieldGoalsMade/fieldGoalsAttempted, NULL)) AS avg_fg_pct,
          AVG(IF(threePointersAttempted>0, threePointersMade/threePointersAttempted, NULL)) AS avg_3p_pct,
          AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct
        FROM `{client.project}.{RAW_TABLE}`
        WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
        GROUP BY month_year, season_year
        ORDER BY season_year DESC, month_year DESC
        LIMIT {int(limit_months)}
        """
        rows = list(client.query(query).result())
        return {
            "status": "success",
            "player": player_name,
            "months": [dict(row) for row in rows],
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def analyze_player_efficiency(player_name: str, season_year: Optional[str] = None, last_n_games: int = 20, project_id: Optional[str] = None) -> Dict[str, Any]:
    """Comprehensive efficiency analysis using advanced basketball metrics.

    This tool provides detailed efficiency analysis including True Shooting percentage,
    shooting trends, volume vs. efficiency trade-offs, and performance consistency.
    Use this for deep dives into how efficiently a player scores and contributes.

    Args:
        player_name: Player name (case-insensitive, partial matches work)
        season_year: Optional season filter in format "YYYY-YY"
        last_n_games: Number of recent games to analyze (default: 20)
        project_id: Optional GCP project ID

    Returns:
        dict with keys:
        - status: "success" or "error"
        - player: Player name searched
        - season_year: Season filter applied (if any)
        - summary: Detailed efficiency analysis including:
          * overall_efficiency: Overall efficiency metrics
          * shooting_analysis: Shooting efficiency breakdown
          * volume_analysis: Shot volume vs. efficiency
          * consistency_metrics: Performance consistency measures
          * best_game: Most efficient game performance
          * worst_game: Least efficient game performance
        - message: Error message if status is "error"

    Example Usage:
        - "How efficient has Curry been in his last 15 games?"
        - "Analyze LeBron's shooting efficiency this season"
        - "Is Giannis more efficient on high or low shot volume?"
    """
    try:
        # Fetch recent game logs
        stats_resp = get_player_stats(player_name=player_name, season_year=season_year, limit=last_n_games, project_id=project_id)
        if stats_resp.get("status") != "success" or not stats_resp.get("records"):
            return stats_resp

        analyzer = EfficiencyAnalyzer()
        for rec in stats_resp["records"]:
            game_date = rec.get("game_date")
            if isinstance(game_date, str):
                # game_date comes as 'YYYY-MM-DD'
                game_date_obj = datetime.strptime(game_date, "%Y-%m-%d").date()
            elif isinstance(game_date, datetime):
                game_date_obj = game_date.date()
            elif isinstance(game_date, date):
                game_date_obj = game_date
            else:
                continue

            stats = _row_to_player_game_stats(rec)
            analyzer.add_game_from_stats(game_date_obj, stats)

        summary = analyzer.get_efficiency_summary()
        return {"status": "success", "player": player_name, "season_year": season_year, "summary": summary}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def analyze_player_defense(player_name: str, season_year: Optional[str] = None, last_n_games: int = 20, project_id: Optional[str] = None) -> Dict[str, Any]:
    """Comprehensive defensive impact analysis for a player.

    This tool evaluates a player's defensive contributions including steals, blocks,
    rebounds, and overall defensive impact. Provides per-36 minute rates and
    defensive strengths assessment. Use this for defensive-focused analysis.

    Args:
        player_name: Player name (case-insensitive, partial matches work)
        season_year: Optional season filter in format "YYYY-YY"
        last_n_games: Number of recent games to analyze (default: 20)
        project_id: Optional GCP project ID

    Returns:
        dict with keys:
        - status: "success" or "error"
        - player: Player name searched
        - season_year: Season filter applied (if any)
        - latest_game_analysis: Detailed analysis of most recent game including:
          * defensive_strengths: Primary defensive skills
          * impact_score: Overall defensive impact rating
          * steal_impact, block_impact, rebound_impact: Specific skill impacts
        - recent_sample_summary: Averages across recent games including:
          * games_analyzed, avg_minutes
          * steals_per_36, blocks_per_36, def_reb_per_36, fouls_per_36
        - message: Error message if status is "error"

    Example Usage:
        - "How good is Giannis defensively this season?"
        - "Analyze Draymond Green's defensive impact"
        - "What are Rudy Gobert's defensive strengths?"
    """
    try:
        stats_resp = get_player_stats(player_name=player_name, season_year=season_year, limit=last_n_games, project_id=project_id)
        if stats_resp.get("status") != "success" or not stats_resp.get("records"):
            return stats_resp

        # Aggregate recent games by simple averages to provide context + last game detailed analysis
        games: List[PlayerGameStats] = []
        for rec in stats_resp["records"]:
            games.append(_row_to_player_game_stats(rec))

        if not games:
            return {"status": "error", "message": "No games available for analysis"}

        # Analyze most recent game and provide recent averages
        latest_game = games[0]
        latest_analysis = analyze_defensive_strengths(latest_game)

        # Compute simple per-36 averages across sample
        total_minutes = sum(g.minutes_played for g in games) or 1.0
        agg = PlayerGameStats()
        for g in games:
            agg.steals += g.steals
            agg.blocks += g.blocks
            agg.rebounds_defensive += g.rebounds_defensive
            agg.fouls_personal += g.fouls_personal
            agg.minutes_played += g.minutes_played

        sample_summary = {
            "games_analyzed": len(games),
            "avg_minutes": round(total_minutes / len(games), 1),
            "steals_per_36": round((agg.steals / agg.minutes_played) * 36.0, 2) if agg.minutes_played else None,
            "blocks_per_36": round((agg.blocks / agg.minutes_played) * 36.0, 2) if agg.minutes_played else None,
            "def_reb_per_36": round((agg.rebounds_defensive / agg.minutes_played) * 36.0, 2) if agg.minutes_played else None,
            "fouls_per_36": round((agg.fouls_personal / agg.minutes_played) * 36.0, 2) if agg.minutes_played else None,
        }

        return {
            "status": "success",
            "player": player_name,
            "season_year": season_year,
            "latest_game_analysis": latest_analysis,
            "recent_sample_summary": sample_summary,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def compare_players_advanced_metrics(
    player_names: List[str], 
    season_year: Optional[str] = None,
    metric_type: str = "all",
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Side-by-side comparison of advanced metrics between multiple players.
    
    This tool compares players across various statistical categories with rankings
    and percentages. Perfect for player debates, draft analysis, trade evaluations,
    or identifying the best performers in specific areas.
    
    Args:
        player_names: List of player names to compare (minimum 2 players)
                     Examples: ["LeBron James", "Kevin Durant", "Giannis"]
        season_year: Optional season filter in format "YYYY-YY"
        metric_type: Type of metrics to focus on:
                    - "scoring": Points, shooting percentages, True Shooting %
                    - "defensive": Steals, blocks, defensive rebounds, defensive impact
                    - "efficiency": Per-minute production rates
                    - "all": Comprehensive comparison across all categories
        project_id: Optional GCP project ID
        
    Returns:
        dict with keys:
        - status: "success" or "error"
        - players: List of players compared
        - season_year: Season filter applied (if any)
        - metric_type: Type of comparison performed
        - comparison_data: Statistical data for each player
        - rankings: Player rankings for each metric (1 = best)
        - count: Number of players successfully found
        - message: Error message if status is "error"

    Example Usage:
        - "Compare LeBron James and Michael Jordan's scoring"
        - "Who's better defensively: Giannis or Rudy Gobert?"
        - "Compare the efficiency of Curry, Dame, and Trae Young"
    """
    try:
        if len(player_names) < 2:
            return {"status": "error", "message": "At least 2 players required for comparison"}
        
        client = _bq_client(project_id)
        season_filter = f"AND season_year = '{season_year}'" if season_year else ""
        
        # Build player name conditions
        name_conditions = []
        for name in player_names:
            name_esc = name.replace("'", "\\'")
            name_conditions.append(f"LOWER(personName) LIKE LOWER('%{name_esc}%')")
        
        player_filter = " OR ".join(name_conditions)
        
        # Select metrics based on type
        if metric_type == "scoring":
            metrics_query = """
                AVG(points) AS avg_points,
                AVG(IF(fieldGoalsAttempted>0, fieldGoalsMade/fieldGoalsAttempted, NULL)) AS avg_fg_pct,
                AVG(IF(threePointersAttempted>0, threePointersMade/threePointersAttempted, NULL)) AS avg_3p_pct,
                AVG(IF(freeThrowsAttempted>0, freeThrowsMade/freeThrowsAttempted, NULL)) AS avg_ft_pct,
                AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct
            """
        elif metric_type == "defensive":
            metrics_query = """
                AVG(steals) AS avg_steals,
                AVG(blocks) AS avg_blocks,
                AVG(reboundsDefensive) AS avg_def_rebounds,
                AVG(foulsPersonal) AS avg_fouls,
                AVG(SAFE_DIVIDE(steals + blocks + reboundsDefensive, minutes)) AS avg_def_impact
            """
        elif metric_type == "efficiency":
            metrics_query = """
                AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct,
                AVG(SAFE_DIVIDE(points, minutes)) AS avg_points_per_minute,
                AVG(SAFE_DIVIDE(assists, minutes)) AS avg_assists_per_minute,
                AVG(SAFE_DIVIDE(reboundsTotal, minutes)) AS avg_rebounds_per_minute
            """
        else:  # "all"
            metrics_query = """
                AVG(points) AS avg_points,
                AVG(reboundsTotal) AS avg_rebounds,
                AVG(assists) AS avg_assists,
                AVG(steals) AS avg_steals,
                AVG(blocks) AS avg_blocks,
                AVG(IF(fieldGoalsAttempted>0, fieldGoalsMade/fieldGoalsAttempted, NULL)) AS avg_fg_pct,
                AVG(IF(threePointersAttempted>0, threePointersMade/threePointersAttempted, NULL)) AS avg_3p_pct,
                AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct,
                AVG(minutes) AS avg_minutes,
                COUNT(1) AS games_played
            """
        
        query = f"""
        SELECT
          personName,
          {metrics_query}
        FROM `{client.project}.{RAW_TABLE}`
        WHERE ({player_filter})
        {season_filter}
        GROUP BY personName
        ORDER BY avg_points DESC
        """
        
        rows = list(client.query(query).result())
        records = [dict(row) for row in rows]
        
        # Calculate rankings for each metric
        rankings = {}
        if records:
            for metric in records[0].keys():
                if metric != 'personName' and records[0][metric] is not None:
                    sorted_records = sorted(records, key=lambda x: x[metric] or 0, reverse=True)
                    rankings[metric] = {rec['personName']: i+1 for i, rec in enumerate(sorted_records)}
        
        return {
            "status": "success",
            "players": player_names,
            "season_year": season_year,
            "metric_type": metric_type,
            "comparison_data": records,
            "rankings": rankings,
            "count": len(records)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def analyze_team_performance_trends(
    team_identifier: str,
    season_year: Optional[str] = None,
    analysis_period: str = "season",
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze team performance trends over time.
    
    Args:
        team_identifier: Team ID, tricode, or slug
        season_year: Optional season filter
        analysis_period: Analysis period ("month", "quarter", "season")
        project_id: GCP project ID
        
    Returns:
        Team performance analysis
    """
    try:
        client = _bq_client(project_id)
        
        # Build team identifier predicate
        if team_identifier.isdigit():
            team_pred = f"TEAM_ID = {int(team_identifier)}"
        elif len(team_identifier) == 3:
            team_pred = f"UPPER(TEAM_ABBREVIATION) = UPPER('{team_identifier}')"
        else:
            team_pred = f"LOWER(TEAM_NAME) = LOWER('{team_identifier}')"
        
        season_filter = f"AND SEASON_YEAR = '{season_year}'" if season_year else ""
        
        # Build time grouping based on analysis_period
        if analysis_period == "month":
            time_group = "FORMAT_DATE('%Y-%m', GAME_DATE)"
            time_label = "month"
        elif analysis_period == "quarter":
            time_group = "CONCAT(SEASON_YEAR, '-Q', CAST(CEIL(EXTRACT(MONTH FROM GAME_DATE)/3) AS STRING))"
            time_label = "quarter"
        else:  # "season"
            time_group = "SEASON_YEAR"
            time_label = "season"
        
        query = f"""
        SELECT
          {time_group} AS {time_label},
          COUNT(1) AS games_played,
          AVG(PTS) AS avg_points,
          AVG(REB) AS avg_rebounds,
          AVG(AST) AS avg_assists,
          AVG(STL) AS avg_steals,
          AVG(BLK) AS avg_blocks,
          AVG(TOV) AS avg_turnovers,
          AVG(IF(FGA>0, FGM/FGA, NULL)) AS avg_fg_pct,
          AVG(IF(FG3A>0, FG3M/FG3A, NULL)) AS avg_3p_pct,
          AVG(PLUS_MINUS) AS avg_plus_minus,
          SUM(CASE WHEN PLUS_MINUS > 0 THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN PLUS_MINUS < 0 THEN 1 ELSE 0 END) AS losses
        FROM `{client.project}.{TEAM_STATS_TABLE}`
        WHERE {team_pred}
        {season_filter}
        GROUP BY {time_group}
        ORDER BY {time_group} DESC
        """
        
        rows = list(client.query(query).result())
        records = [dict(row) for row in rows]
        
        # Calculate trends
        trends = {}
        if len(records) >= 2:
            for metric in ['avg_points', 'avg_rebounds', 'avg_assists', 'avg_fg_pct', 'avg_3p_pct']:
                if records[0].get(metric) and records[1].get(metric):
                    current = records[0][metric]
                    previous = records[1][metric]
                    change = current - previous
                    change_pct = (change / previous * 100) if previous != 0 else 0
                    trends[metric] = {
                        'current': current,
                        'previous': previous,
                        'change': change,
                        'change_percent': change_pct,
                        'trend': 'improving' if change > 0 else 'declining' if change < 0 else 'stable'
                    }
        
        return {
            "status": "success",
            "team": team_identifier,
            "season_year": season_year,
            "analysis_period": analysis_period,
            "performance_data": records,
            "trends": trends,
            "total_periods": len(records)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def analyze_player_efficiency_deep_dive(
    player_name: str,
    season_year: Optional[str] = None,
    analysis_type: str = "comprehensive",
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Deep dive analysis of player efficiency metrics.
    
    Args:
        player_name: Player name
        season_year: Optional season filter
        analysis_type: Analysis type ("scoring", "defensive", "comprehensive")
        project_id: GCP project ID
        
    Returns:
        Deep dive efficiency analysis
    """
    try:
        client = _bq_client(project_id)
        name_esc = player_name.replace("'", "\\'")
        season_filter = f"AND season_year = '{season_year}'" if season_year else ""
        
        if analysis_type == "scoring":
            query = f"""
            SELECT
              AVG(points) AS avg_points,
              AVG(IF(fieldGoalsAttempted>0, fieldGoalsMade/fieldGoalsAttempted, NULL)) AS avg_fg_pct,
              AVG(IF(threePointersAttempted>0, threePointersMade/threePointersAttempted, NULL)) AS avg_3p_pct,
              AVG(IF(freeThrowsAttempted>0, freeThrowsMade/freeThrowsAttempted, NULL)) AS avg_ft_pct,
              AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct,
              AVG(SAFE_DIVIDE(points, minutes)) AS points_per_minute,
              AVG(fieldGoalsAttempted) AS avg_fga,
              AVG(threePointersAttempted) AS avg_3pa,
              AVG(freeThrowsAttempted) AS avg_fta,
              COUNT(1) AS games_played,
              SUM(points) AS total_points,
              SUM(fieldGoalsMade) AS total_fgm,
              SUM(threePointersMade) AS total_3pm,
              SUM(freeThrowsMade) AS total_ftm
            FROM `{client.project}.{RAW_TABLE}`
            WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            """
        elif analysis_type == "defensive":
            query = f"""
            SELECT
              AVG(steals) AS avg_steals,
              AVG(blocks) AS avg_blocks,
              AVG(reboundsDefensive) AS avg_def_rebounds,
              AVG(foulsPersonal) AS avg_fouls,
              AVG(SAFE_DIVIDE(steals, minutes)) AS steals_per_minute,
              AVG(SAFE_DIVIDE(blocks, minutes)) AS blocks_per_minute,
              AVG(SAFE_DIVIDE(reboundsDefensive, minutes)) AS def_rebounds_per_minute,
              AVG(SAFE_DIVIDE(foulsPersonal, minutes)) AS fouls_per_minute,
              COUNT(1) AS games_played,
              SUM(steals) AS total_steals,
              SUM(blocks) AS total_blocks,
              SUM(reboundsDefensive) AS total_def_rebounds
            FROM `{client.project}.{RAW_TABLE}`
            WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            """
        else:  # "comprehensive"
            query = f"""
            SELECT
              AVG(points) AS avg_points,
              AVG(reboundsTotal) AS avg_rebounds,
              AVG(assists) AS avg_assists,
              AVG(steals) AS avg_steals,
              AVG(blocks) AS avg_blocks,
              AVG(turnovers) AS avg_turnovers,
              AVG(IF(fieldGoalsAttempted>0, fieldGoalsMade/fieldGoalsAttempted, NULL)) AS avg_fg_pct,
              AVG(IF(threePointersAttempted>0, threePointersMade/threePointersAttempted, NULL)) AS avg_3p_pct,
              AVG(IF(freeThrowsAttempted>0, freeThrowsMade/freeThrowsAttempted, NULL)) AS avg_ft_pct,
              AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct,
              AVG(SAFE_DIVIDE(points, minutes)) AS points_per_minute,
              AVG(SAFE_DIVIDE(reboundsTotal, minutes)) AS rebounds_per_minute,
              AVG(SAFE_DIVIDE(assists, minutes)) AS assists_per_minute,
              AVG(SAFE_DIVIDE(steals, minutes)) AS steals_per_minute,
              AVG(SAFE_DIVIDE(blocks, minutes)) AS blocks_per_minute,
              AVG(SAFE_DIVIDE(turnovers, minutes)) AS turnovers_per_minute,
              AVG(minutes) AS avg_minutes,
              COUNT(1) AS games_played
            FROM `{client.project}.{RAW_TABLE}`
            WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            """
        
        rows = list(client.query(query).result())
        if not rows:
            return {"status": "error", "message": f"No data found for player: {player_name}"}
        
        result = dict(rows[0])
        
        # Add efficiency grades
        if 'avg_ts_pct' in result and result['avg_ts_pct']:
            ts_pct = result['avg_ts_pct']
            if ts_pct >= 0.65:
                result['ts_grade'] = 'A+'
            elif ts_pct >= 0.60:
                result['ts_grade'] = 'A'
            elif ts_pct >= 0.55:
                result['ts_grade'] = 'B+'
            elif ts_pct >= 0.50:
                result['ts_grade'] = 'B'
            elif ts_pct >= 0.45:
                result['ts_grade'] = 'C+'
            else:
                result['ts_grade'] = 'C'
        
        return {
            "status": "success",
            "player": player_name,
            "season_year": season_year,
            "analysis_type": analysis_type,
            "efficiency_metrics": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def analyze_player_performance_by_game_situation(
    player_name: str,
    season_year: Optional[str] = None,
    situation_type: str = "all",
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze player performance by different game situations.
    
    Args:
        player_name: Player name
        season_year: Optional season filter
        situation_type: Situation type ("clutch", "home_away", "back_to_back", "all")
        project_id: GCP project ID
        
    Returns:
        Situation-based performance analysis
    """
    try:
        client = _bq_client(project_id)
        name_esc = player_name.replace("'", "\\'")
        season_filter = f"AND season_year = '{season_year}'" if season_year else ""
        
        if situation_type == "home_away":
            query = f"""
            SELECT
              CASE 
                WHEN teamTricode = SUBSTR(matchup, 1, 3) THEN 'Home'
                ELSE 'Away'
              END AS game_location,
              COUNT(1) AS games_played,
              AVG(points) AS avg_points,
              AVG(reboundsTotal) AS avg_rebounds,
              AVG(assists) AS avg_assists,
              AVG(IF(fieldGoalsAttempted>0, fieldGoalsMade/fieldGoalsAttempted, NULL)) AS avg_fg_pct,
              AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct,
              AVG(plusMinusPoints) AS avg_plus_minus
            FROM `{client.project}.{RAW_TABLE}`
            WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            GROUP BY game_location
            """
        elif situation_type == "clutch":
            # Define clutch as games with close scores (within 5 points)
            query = f"""
            SELECT
              CASE 
                WHEN ABS(plusMinusPoints) <= 5 THEN 'Clutch'
                ELSE 'Non-Clutch'
              END AS game_situation,
              COUNT(1) AS games_played,
              AVG(points) AS avg_points,
              AVG(reboundsTotal) AS avg_rebounds,
              AVG(assists) AS avg_assists,
              AVG(IF(fieldGoalsAttempted>0, fieldGoalsMade/fieldGoalsAttempted, NULL)) AS avg_fg_pct,
              AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct,
              AVG(plusMinusPoints) AS avg_plus_minus
            FROM `{client.project}.{RAW_TABLE}`
            WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            GROUP BY game_situation
            """
        else:  # "all"
            query = f"""
            SELECT
              CASE 
                WHEN teamTricode = SUBSTR(matchup, 1, 3) THEN 'Home'
                ELSE 'Away'
              END AS game_location,
              CASE 
                WHEN ABS(plusMinusPoints) <= 5 THEN 'Clutch'
                ELSE 'Non-Clutch'
              END AS game_situation,
              COUNT(1) AS games_played,
              AVG(points) AS avg_points,
              AVG(reboundsTotal) AS avg_rebounds,
              AVG(assists) AS avg_assists,
              AVG(IF(fieldGoalsAttempted>0, fieldGoalsMade/fieldGoalsAttempted, NULL)) AS avg_fg_pct,
              AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct,
              AVG(plusMinusPoints) AS avg_plus_minus
            FROM `{client.project}.{RAW_TABLE}`
            WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            GROUP BY game_location, game_situation
            ORDER BY game_location, game_situation
            """
        
        rows = list(client.query(query).result())
        records = [dict(row) for row in rows]
        
        return {
            "status": "success",
            "player": player_name,
            "season_year": season_year,
            "situation_type": situation_type,
            "situation_analysis": records,
            "count": len(records)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def predict_player_performance(
    player_name: str,
    prediction_type: str = "next_game",
    historical_games: int = 20,
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Predict player performance based on historical data.
    
    Args:
        player_name: Player name
        prediction_type: Prediction type ("next_game", "season_avg", "trend")
        historical_games: Number of historical games to analyze
        project_id: GCP project ID
        
    Returns:
        Performance prediction results
    """
    try:
        client = _bq_client(project_id)
        name_esc = player_name.replace("'", "\\'")
        
        # Get recent performance data
        query = f"""
        SELECT
          game_date,
          points,
          reboundsTotal,
          assists,
          steals,
          blocks,
          turnovers,
          minutes,
          IF(fieldGoalsAttempted>0, fieldGoalsMade/fieldGoalsAttempted, NULL) AS fg_pct,
          SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted)) AS ts_pct,
          plusMinusPoints
        FROM `{client.project}.{RAW_TABLE}`
        WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
        ORDER BY game_date DESC
        LIMIT {historical_games}
        """
        
        rows = list(client.query(query).result())
        if not rows:
            return {"status": "error", "message": f"No historical data found for player: {player_name}"}
        
        records = [dict(row) for row in rows]
        
        # Calculate predictions based on type
        predictions = {}
        
        if prediction_type == "next_game":
            # Simple average of recent games
            recent_games = records[:10]  # Last 10 games
            if recent_games:
                predictions = {
                    'predicted_points': round(sum(r['points'] for r in recent_games) / len(recent_games), 1),
                    'predicted_rebounds': round(sum(r['reboundsTotal'] for r in recent_games) / len(recent_games), 1),
                    'predicted_assists': round(sum(r['assists'] for r in recent_games) / len(recent_games), 1),
                    'predicted_minutes': round(sum(r['minutes'] for r in recent_games) / len(recent_games), 1),
                    'confidence': 'high' if len(recent_games) >= 8 else 'medium'
                }
        
        elif prediction_type == "season_avg":
            # Season average prediction
            if records:
                predictions = {
                    'predicted_points': round(sum(r['points'] for r in records) / len(records), 1),
                    'predicted_rebounds': round(sum(r['reboundsTotal'] for r in records) / len(records), 1),
                    'predicted_assists': round(sum(r['assists'] for r in records) / len(records), 1),
                    'predicted_minutes': round(sum(r['minutes'] for r in records) / len(records), 1),
                    'confidence': 'medium'
                }
        
        elif prediction_type == "trend":
            # Trend-based prediction
            if len(records) >= 5:
                recent_avg = {
                    'points': sum(r['points'] for r in records[:5]) / 5,
                    'rebounds': sum(r['reboundsTotal'] for r in records[:5]) / 5,
                    'assists': sum(r['assists'] for r in records[:5]) / 5
                }
                
                older_avg = {
                    'points': sum(r['points'] for r in records[5:10]) / 5 if len(records) >= 10 else recent_avg['points'],
                    'rebounds': sum(r['reboundsTotal'] for r in records[5:10]) / 5 if len(records) >= 10 else recent_avg['rebounds'],
                    'assists': sum(r['assists'] for r in records[5:10]) / 5 if len(records) >= 10 else recent_avg['assists']
                }
                
                # Apply trend factor
                trend_factor = 1.1  # 10% improvement if trending up
                predictions = {
                    'predicted_points': round(recent_avg['points'] * trend_factor, 1),
                    'predicted_rebounds': round(recent_avg['rebounds'] * trend_factor, 1),
                    'predicted_assists': round(recent_avg['assists'] * trend_factor, 1),
                    'trend_direction': 'improving' if recent_avg['points'] > older_avg['points'] else 'declining',
                    'confidence': 'medium'
                }
        
        return {
            "status": "success",
            "player": player_name,
            "prediction_type": prediction_type,
            "historical_games": len(records),
            "predictions": predictions,
            "recent_performance": records[:5]  # Last 5 games for context
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def calculate_advanced_basketball_metrics(
    player_name: str,
    season_year: Optional[str] = None,
    metrics: Optional[List[str]] = None,
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Calculate advanced basketball metrics for a player.
    
    Args:
        player_name: Player name
        season_year: Optional season filter
        metrics: List of metrics to calculate (if None, calculates all)
        project_id: GCP project ID
        
    Returns:
        Advanced metrics calculation results
    """
    try:
        client = _bq_client(project_id)
        name_esc = player_name.replace("'", "\\'")
        season_filter = f"AND season_year = '{season_year}'" if season_year else ""
        
        # Default metrics if none specified
        if not metrics:
            metrics = ["per", "ts_pct", "efg_pct", "usage_rate", "defensive_impact"]
        
        # Build query based on requested metrics
        select_clauses = []
        
        if "per" in metrics:
            select_clauses.append("""
                AVG((
                  (fieldGoalsMade + 0.5 * threePointersMade + freeThrowsMade + 
                   steals + 0.5 * assists + 0.5 * blocks + reboundsOffensive + reboundsDefensive) -
                  (0.5 * foulsPersonal + turnovers + 
                   (fieldGoalsAttempted - fieldGoalsMade) + 
                   0.5 * (freeThrowsAttempted - freeThrowsMade))
                ) / NULLIF(minutes, 0) * 30.0) AS player_efficiency_rating
            """)
        
        if "ts_pct" in metrics:
            select_clauses.append("""
                AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS true_shooting_pct
            """)
        
        if "efg_pct" in metrics:
            select_clauses.append("""
                AVG(SAFE_DIVIDE(fieldGoalsMade + 0.5 * threePointersMade, fieldGoalsAttempted)) AS effective_fg_pct
            """)
        
        if "usage_rate" in metrics:
            select_clauses.append("""
                AVG(SAFE_DIVIDE(fieldGoalsAttempted + 0.44*freeThrowsAttempted + turnovers, minutes)) AS usage_rate
            """)
        
        if "defensive_impact" in metrics:
            select_clauses.append("""
                AVG(SAFE_DIVIDE(steals + blocks + reboundsDefensive, minutes)) AS defensive_impact_score
            """)
        
        if not select_clauses:
            return {"status": "error", "message": "No valid metrics specified"}
        
        query = f"""
        SELECT
          {', '.join(select_clauses)},
          COUNT(1) AS games_played,
          AVG(minutes) AS avg_minutes
        FROM `{client.project}.{RAW_TABLE}`
        WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
        {season_filter}
        """
        
        rows = list(client.query(query).result())
        if not rows:
            return {"status": "error", "message": f"No data found for player: {player_name}"}
        
        result = dict(rows[0])
        
        # Add percentile rankings if possible
        percentiles = {}
        for metric in metrics:
            if metric in result and result[metric] is not None:
                # This would require league-wide data for true percentiles
                # For now, provide basic grading
                value = result[metric]
                if metric == "true_shooting_pct":
                    if value >= 0.65:
                        percentiles[metric] = "Elite"
                    elif value >= 0.55:
                        percentiles[metric] = "Above Average"
                    elif value >= 0.45:
                        percentiles[metric] = "Average"
                    else:
                        percentiles[metric] = "Below Average"
        
        return {
            "status": "success",
            "player": player_name,
            "season_year": season_year,
            "advanced_metrics": result,
            "percentiles": percentiles,
            "metrics_calculated": metrics
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def analyze_statistical_correlations(
    player_name: str,
    season_year: Optional[str] = None,
    correlation_type: str = "performance",
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze statistical correlations for a player.
    
    Args:
        player_name: Player name
        season_year: Optional season filter
        correlation_type: Correlation type ("performance", "efficiency", "defensive")
        project_id: GCP project ID
        
    Returns:
        Correlation analysis results
    """
    try:
        client = _bq_client(project_id)
        name_esc = player_name.replace("'", "\\'")
        season_filter = f"AND season_year = '{season_year}'" if season_year else ""
        
        if correlation_type == "performance":
            query = f"""
            SELECT
              CORR(points, minutes) AS points_minutes_corr,
              CORR(points, fieldGoalsAttempted) AS points_fga_corr,
              CORR(reboundsTotal, minutes) AS rebounds_minutes_corr,
              CORR(assists, minutes) AS assists_minutes_corr,
              CORR(points, assists) AS points_assists_corr,
              CORR(reboundsTotal, assists) AS rebounds_assists_corr,
              COUNT(1) AS games_analyzed
            FROM `{client.project}.{RAW_TABLE}`
            WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            """
        elif correlation_type == "efficiency":
            query = f"""
            SELECT
              CORR(points, SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS points_ts_corr,
              CORR(fieldGoalsMade, fieldGoalsAttempted) AS fg_made_attempted_corr,
              CORR(threePointersMade, threePointersAttempted) AS three_made_attempted_corr,
              CORR(freeThrowsMade, freeThrowsAttempted) AS ft_made_attempted_corr,
              CORR(minutes, SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS minutes_ts_corr,
              COUNT(1) AS games_analyzed
            FROM `{client.project}.{RAW_TABLE}`
            WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            """
        else:  # "defensive"
            query = f"""
            SELECT
              CORR(steals, minutes) AS steals_minutes_corr,
              CORR(blocks, minutes) AS blocks_minutes_corr,
              CORR(reboundsDefensive, minutes) AS def_rebounds_minutes_corr,
              CORR(foulsPersonal, minutes) AS fouls_minutes_corr,
              CORR(steals, blocks) AS steals_blocks_corr,
              CORR(reboundsDefensive, foulsPersonal) AS def_rebounds_fouls_corr,
              COUNT(1) AS games_analyzed
            FROM `{client.project}.{RAW_TABLE}`
            WHERE LOWER(personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            """
        
        rows = list(client.query(query).result())
        if not rows:
            return {"status": "error", "message": f"No data found for player: {player_name}"}
        
        result = dict(rows[0])
        
        # Interpret correlations
        interpretations = {}
        for key, value in result.items():
            if key != 'games_analyzed' and value is not None:
                if abs(value) >= 0.7:
                    strength = "Strong"
                elif abs(value) >= 0.4:
                    strength = "Moderate"
                elif abs(value) >= 0.2:
                    strength = "Weak"
                else:
                    strength = "Very Weak"
                
                interpretations[key] = {
                    'correlation': value,
                    'strength': strength,
                    'direction': 'Positive' if value > 0 else 'Negative'
                }
        
        return {
            "status": "success",
            "player": player_name,
            "season_year": season_year,
            "correlation_type": correlation_type,
            "correlations": result,
            "interpretations": interpretations,
            "games_analyzed": result.get('games_analyzed', 0)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def cluster_players_by_playing_style(
    position: Optional[str] = None,
    season_year: Optional[str] = None,
    cluster_count: int = 5,
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Cluster players by playing style using statistical similarity.
    
    Args:
        position: Optional position filter
        season_year: Optional season filter
        cluster_count: Number of clusters to create
        project_id: GCP project ID
        
    Returns:
        Player clustering results
    """
    try:
        client = _bq_client(project_id)
        season_filter = f"AND season_year = '{season_year}'" if season_year else ""
        position_filter = f"AND position = '{position}'" if position else ""
        
        # Get player statistics for clustering
        query = f"""
        SELECT
          personName,
          AVG(points) AS avg_points,
          AVG(reboundsTotal) AS avg_rebounds,
          AVG(assists) AS avg_assists,
          AVG(steals) AS avg_steals,
          AVG(blocks) AS avg_blocks,
          AVG(SAFE_DIVIDE(points, 2*(fieldGoalsAttempted + 0.44*freeThrowsAttempted))) AS avg_ts_pct,
          AVG(SAFE_DIVIDE(assists, minutes)) AS assists_per_minute,
          AVG(SAFE_DIVIDE(reboundsTotal, minutes)) AS rebounds_per_minute,
          AVG(SAFE_DIVIDE(steals + blocks, minutes)) AS defensive_activity,
          COUNT(1) AS games_played
        FROM `{client.project}.{RAW_TABLE}`
        WHERE games_played >= 10
        {season_filter}
        {position_filter}
        GROUP BY personName
        HAVING games_played >= 10
        ORDER BY avg_points DESC
        LIMIT 100
        """
        
        rows = list(client.query(query).result())
        if not rows:
            return {"status": "error", "message": "No player data found for clustering"}
        
        records = [dict(row) for row in rows]
        
        # Simple clustering based on playing style characteristics
        clusters = {
            'scorers': [],
            'playmakers': [],
            'rebounders': [],
            'defenders': [],
            'all_around': []
        }
        
        for player in records:
            points = player['avg_points'] or 0
            assists = player['assists_per_minute'] or 0
            rebounds = player['rebounds_per_minute'] or 0
            defensive = player['defensive_activity'] or 0
            
            # Determine player type based on statistical profile
            if points >= 20 and assists < 0.3:
                clusters['scorers'].append(player)
            elif assists >= 0.4:
                clusters['playmakers'].append(player)
            elif rebounds >= 0.4:
                clusters['rebounders'].append(player)
            elif defensive >= 0.2:
                clusters['defenders'].append(player)
            else:
                clusters['all_around'].append(player)
        
        # Calculate cluster statistics
        cluster_stats = {}
        for cluster_name, players in clusters.items():
            if players:
                cluster_stats[cluster_name] = {
                    'count': len(players),
                    'avg_points': sum(p['avg_points'] or 0 for p in players) / len(players),
                    'avg_assists': sum(p['avg_assists'] or 0 for p in players) / len(players),
                    'avg_rebounds': sum(p['avg_rebounds'] or 0 for p in players) / len(players),
                    'top_players': sorted(players, key=lambda x: x['avg_points'] or 0, reverse=True)[:5]
                }
        
        return {
            "status": "success",
            "position": position,
            "season_year": season_year,
            "total_players": len(records),
            "clusters": cluster_stats,
            "cluster_definitions": {
                'scorers': 'High scoring, low assist players',
                'playmakers': 'High assist rate players',
                'rebounders': 'High rebounding rate players',
                'defenders': 'High defensive activity players',
                'all_around': 'Balanced statistical profile players'
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Expose ADK tools
run_query_tool = FunctionTool(run_query)
get_query_status_tool = FunctionTool(get_query_status)
get_query_results_tool = FunctionTool(get_query_results)

get_player_stats_tool = FunctionTool(get_player_stats)
get_player_stats_by_season_tool = FunctionTool(get_player_stats_by_season)
get_team_stats_tool = FunctionTool(get_team_stats)
get_player_monthly_trends_tool = FunctionTool(get_player_monthly_trends)
analyze_player_efficiency_tool = FunctionTool(analyze_player_efficiency)
analyze_player_defense_tool = FunctionTool(analyze_player_defense)

# New advanced analysis tools
compare_players_advanced_metrics_tool = FunctionTool(compare_players_advanced_metrics)
analyze_team_performance_trends_tool = FunctionTool(analyze_team_performance_trends)
analyze_player_efficiency_deep_dive_tool = FunctionTool(analyze_player_efficiency_deep_dive)
analyze_player_performance_by_game_situation_tool = FunctionTool(analyze_player_performance_by_game_situation)
predict_player_performance_tool = FunctionTool(predict_player_performance)
calculate_advanced_basketball_metrics_tool = FunctionTool(calculate_advanced_basketball_metrics)
analyze_statistical_correlations_tool = FunctionTool(analyze_statistical_correlations)
cluster_players_by_playing_style_tool = FunctionTool(cluster_players_by_playing_style)


def analyze_player_team_impact(
    player_name: str,
    season_year: Optional[str] = None,
    impact_type: str = "comprehensive",
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze how much a player contributes to their team's overall performance.
    
    This advanced tool combines individual player stats with team totals to calculate
    contribution percentages, impact ratios, and role classifications. Essential for
    understanding a player's value within their team context.
    
    Args:
        player_name: Player name (case-insensitive, partial matches work)
        season_year: Optional season filter in format "YYYY-YY"
        impact_type: Type of impact analysis:
                    - "scoring": Focus on scoring contribution and efficiency
                    - "defensive": Focus on defensive impact and rebounding
                    - "comprehensive": Full analysis across all categories
        project_id: Optional GCP project ID
        
    Returns:
        dict with keys:
        - status: "success" or "error"
        - player: Player name analyzed
        - season_year: Season filter applied (if any)
        - impact_type: Type of analysis performed
        - team_impact_data: Raw statistical data including:
          * games_played, player/team averages
          * contribution ratios (scoring_contribution, assist_contribution, etc.)
          * efficiency metrics
        - impact_analysis: Interpreted analysis including:
          * scoring_impact: Role classification (Primary Scorer, Role Player, etc.)
          * defensive_impact: Defensive role (Defensive Anchor, Good Defender, etc.)
        - message: Error message if status is "error"

    Example Usage:
        - "How much does LeBron contribute to the Lakers' scoring?"
        - "What's Giannis's impact on the Bucks' defense?"
        - "Analyze Curry's overall contribution to Golden State"
    """
    try:
        client = _bq_client(project_id)
        name_esc = player_name.replace("'", "\\'")
        season_filter = f"AND p.season_year = '{season_year}'" if season_year else ""
        
        if impact_type == "scoring":
            query = f"""
            SELECT
              p.personName,
              p.teamTricode,
              COUNT(DISTINCT p.gameId) AS games_played,
              AVG(p.points) AS player_avg_points,
              AVG(t.PTS) AS team_avg_points,
              AVG(t.PTS - p.points) AS team_points_without_player,
              AVG(p.points / NULLIF(t.PTS, 0)) AS player_scoring_contribution,
              AVG(IF(t.PTS > 0, p.points / t.PTS, 0)) AS scoring_share,
              AVG(IF(p.points > 0, t.PTS / p.points, 0)) AS team_efficiency_with_player
            FROM `{client.project}.{RAW_TABLE}` p
            JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
              ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
            WHERE LOWER(p.personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            GROUP BY p.personName, p.teamTricode
            """
        elif impact_type == "defensive":
            query = f"""
            SELECT
              p.personName,
              p.teamTricode,
              COUNT(DISTINCT p.gameId) AS games_played,
              AVG(p.steals + p.blocks) AS player_defensive_activity,
              AVG(t.STL + t.BLK) AS team_defensive_activity,
              AVG((p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0)) AS defensive_contribution_ratio,
              AVG(p.reboundsDefensive) AS player_def_rebounds,
              AVG(t.DREB) AS team_def_rebounds,
              AVG(p.reboundsDefensive / NULLIF(t.DREB, 0)) AS def_rebound_share
            FROM `{client.project}.{RAW_TABLE}` p
            JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
              ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
            WHERE LOWER(p.personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            GROUP BY p.personName, p.teamTricode
            """
        else:  # "comprehensive"
            query = f"""
            SELECT
              p.personName,
              p.teamTricode,
              COUNT(DISTINCT p.gameId) AS games_played,
              AVG(p.points) AS player_avg_points,
              AVG(t.PTS) AS team_avg_points,
              AVG(p.assists) AS player_avg_assists,
              AVG(t.AST) AS team_avg_assists,
              AVG(p.reboundsTotal) AS player_avg_rebounds,
              AVG(t.REB) AS team_avg_rebounds,
              AVG(p.steals + p.blocks) AS player_defensive_activity,
              AVG(t.STL + t.BLK) AS team_defensive_activity,
              AVG(p.points / NULLIF(t.PTS, 0)) AS scoring_contribution,
              AVG(p.assists / NULLIF(t.AST, 0)) AS assist_contribution,
              AVG(p.reboundsTotal / NULLIF(t.REB, 0)) AS rebound_contribution,
              AVG((p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0)) AS defensive_contribution,
              AVG(t.PLUS_MINUS) AS team_avg_plus_minus,
              AVG(p.plusMinusPoints) AS player_avg_plus_minus
            FROM `{client.project}.{RAW_TABLE}` p
            JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
              ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
            WHERE LOWER(p.personName) LIKE LOWER('%{name_esc}%')
            {season_filter}
            GROUP BY p.personName, p.teamTricode
            """
        
        rows = list(client.query(query).result())
        if not rows:
            return {"status": "error", "message": f"No data found for player: {player_name}"}
        
        result = dict(rows[0])
        
        # Calculate impact metrics
        impact_analysis = {}
        if 'scoring_contribution' in result and result['scoring_contribution']:
            share = result['scoring_contribution']
            if share >= 0.25:
                impact_analysis['scoring_impact'] = 'High (Primary Scorer)'
            elif share >= 0.15:
                impact_analysis['scoring_impact'] = 'Medium (Secondary Scorer)'
            elif share >= 0.08:
                impact_analysis['scoring_impact'] = 'Low (Role Player)'
            else:
                impact_analysis['scoring_impact'] = 'Minimal'
        
        if 'defensive_contribution' in result and result['defensive_contribution']:
            def_share = result['defensive_contribution']
            if def_share >= 0.15:
                impact_analysis['defensive_impact'] = 'High (Defensive Anchor)'
            elif def_share >= 0.08:
                impact_analysis['defensive_impact'] = 'Medium (Good Defender)'
            elif def_share >= 0.04:
                impact_analysis['defensive_impact'] = 'Low (Adequate Defender)'
            else:
                impact_analysis['defensive_impact'] = 'Minimal'
        
        return {
            "status": "success",
            "player": player_name,
            "season_year": season_year,
            "impact_type": impact_type,
            "team_impact_data": result,
            "impact_analysis": impact_analysis
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def analyze_lineup_effectiveness(
    team_identifier: str,
    season_year: Optional[str] = None,
    analysis_type: str = "scoring",
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze team lineup effectiveness by combining player and team data.
    
    Args:
        team_identifier: Team ID, tricode, or slug
        season_year: Optional season filter
        analysis_type: Analysis type ("scoring", "defensive", "comprehensive")
        project_id: GCP project ID
        
    Returns:
        Lineup effectiveness analysis
    """
    try:
        client = _bq_client(project_id)
        
        # Build team identifier predicate
        if team_identifier.isdigit():
            team_pred = f"p.teamId = {int(team_identifier)}"
        elif len(team_identifier) == 3:
            team_pred = f"UPPER(p.teamTricode) = UPPER('{team_identifier}')"
        else:
            team_pred = f"LOWER(p.teamSlug) = LOWER('{team_identifier}')"
        
        season_filter = f"AND p.season_year = '{season_year}'" if season_year else ""
        
        if analysis_type == "scoring":
            query = f"""
            SELECT
              p.personName,
              COUNT(DISTINCT p.gameId) AS games_played,
              AVG(p.points) AS avg_points,
              AVG(t.PTS) AS team_avg_points,
              AVG(p.points / NULLIF(t.PTS, 0)) AS scoring_share,
              AVG(p.minutes) AS avg_minutes,
              AVG(p.points / NULLIF(p.minutes, 0)) AS points_per_minute,
              RANK() OVER (ORDER BY AVG(p.points) DESC) AS scoring_rank
            FROM `{client.project}.{RAW_TABLE}` p
            JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
              ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
            WHERE {team_pred}
            {season_filter}
            GROUP BY p.personName
            HAVING games_played >= 10
            ORDER BY avg_points DESC
            """
        elif analysis_type == "defensive":
            query = f"""
            SELECT
              p.personName,
              COUNT(DISTINCT p.gameId) AS games_played,
              AVG(p.steals + p.blocks) AS avg_defensive_activity,
              AVG(t.STL + t.BLK) AS team_defensive_activity,
              AVG((p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0)) AS defensive_share,
              AVG(p.reboundsDefensive) AS avg_def_rebounds,
              AVG(p.minutes) AS avg_minutes,
              AVG((p.steals + p.blocks) / NULLIF(p.minutes, 0)) AS defensive_activity_per_minute,
              RANK() OVER (ORDER BY AVG(p.steals + p.blocks) DESC) AS defensive_rank
            FROM `{client.project}.{RAW_TABLE}` p
            JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
              ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
            WHERE {team_pred}
            {season_filter}
            GROUP BY p.personName
            HAVING games_played >= 10
            ORDER BY avg_defensive_activity DESC
            """
        else:  # "comprehensive"
            query = f"""
            SELECT
              p.personName,
              COUNT(DISTINCT p.gameId) AS games_played,
              AVG(p.points) AS avg_points,
              AVG(p.assists) AS avg_assists,
              AVG(p.reboundsTotal) AS avg_rebounds,
              AVG(p.steals + p.blocks) AS avg_defensive_activity,
              AVG(p.minutes) AS avg_minutes,
              AVG(p.points / NULLIF(t.PTS, 0)) AS scoring_share,
              AVG(p.assists / NULLIF(t.AST, 0)) AS assist_share,
              AVG(p.reboundsTotal / NULLIF(t.REB, 0)) AS rebound_share,
              AVG((p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0)) AS defensive_share,
              AVG(p.plusMinusPoints) AS avg_plus_minus
            FROM `{client.project}.{RAW_TABLE}` p
            JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
              ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
            WHERE {team_pred}
            {season_filter}
            GROUP BY p.personName
            HAVING games_played >= 10
            ORDER BY avg_points DESC
            """
        
        rows = list(client.query(query).result())
        records = [dict(row) for row in rows]
        
        # Calculate team efficiency metrics
        if records:
            total_games = records[0]['games_played']
            avg_team_points = sum(r.get('team_avg_points', 0) for r in records) / len(records)
            
            # Identify key contributors
            key_contributors = []
            for record in records:
                if record.get('scoring_share', 0) >= 0.15:
                    key_contributors.append({
                        'player': record['personName'],
                        'role': 'Primary Scorer',
                        'contribution': f"{record.get('scoring_share', 0):.1%}"
                    })
                elif record.get('defensive_share', 0) >= 0.12:
                    key_contributors.append({
                        'player': record['personName'],
                        'role': 'Defensive Anchor',
                        'contribution': f"{record.get('defensive_share', 0):.1%}"
                    })
        
        return {
            "status": "success",
            "team": team_identifier,
            "season_year": season_year,
            "analysis_type": analysis_type,
            "lineup_data": records,
            "key_contributors": key_contributors,
            "total_players_analyzed": len(records)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def analyze_player_team_synergy(
    player_name: str,
    team_identifier: str,
    season_year: Optional[str] = None,
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze the synergy between a specific player and their team.
    
    Args:
        player_name: Player name
        team_identifier: Team ID, tricode, or slug
        season_year: Optional season filter
        project_id: GCP project ID
        
    Returns:
        Player-team synergy analysis
    """
    try:
        client = _bq_client(project_id)
        name_esc = player_name.replace("'", "\\'")
        season_filter = f"AND p.season_year = '{season_year}'" if season_year else ""
        
        # Build team identifier predicate
        if team_identifier.isdigit():
            team_pred = f"p.teamId = {int(team_identifier)}"
        elif len(team_identifier) == 3:
            team_pred = f"UPPER(p.teamTricode) = UPPER('{team_identifier}')"
        else:
            team_pred = f"LOWER(p.teamSlug) = LOWER('{team_identifier}')"
        
        query = f"""
        SELECT
          p.personName,
          p.teamTricode,
          COUNT(DISTINCT p.gameId) AS games_played,
          AVG(p.points) AS player_avg_points,
          AVG(t.PTS) AS team_avg_points,
          AVG(p.assists) AS player_avg_assists,
          AVG(t.AST) AS team_avg_assists,
          AVG(p.reboundsTotal) AS player_avg_rebounds,
          AVG(t.REB) AS team_avg_rebounds,
          AVG(p.steals + p.blocks) AS player_defensive_activity,
          AVG(t.STL + t.BLK) AS team_defensive_activity,
          AVG(p.points / NULLIF(t.PTS, 0)) AS scoring_contribution,
          AVG(p.assists / NULLIF(t.AST, 0)) AS assist_contribution,
          AVG(p.reboundsTotal / NULLIF(t.REB, 0)) AS rebound_contribution,
          AVG((p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0)) AS defensive_contribution,
          AVG(t.PLUS_MINUS) AS team_avg_plus_minus,
          AVG(p.plusMinusPoints) AS player_avg_plus_minus,
          AVG(CASE WHEN t.PLUS_MINUS > 0 THEN 1 ELSE 0 END) AS team_win_rate,
          AVG(CASE WHEN p.plusMinusPoints > 0 THEN 1 ELSE 0 END) AS player_win_rate
        FROM `{client.project}.{RAW_TABLE}` p
        JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
          ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
        WHERE LOWER(p.personName) LIKE LOWER('%{name_esc}%')
          AND {team_pred}
        {season_filter}
        GROUP BY p.personName, p.teamTricode
        """
        
        rows = list(client.query(query).result())
        if not rows:
            return {"status": "error", "message": f"No data found for player-team combination"}
        
        result = dict(rows[0])
        
        # Calculate synergy metrics
        synergy_analysis = {}
        
        # Scoring synergy
        if result.get('scoring_contribution'):
            scoring_share = result['scoring_contribution']
            if scoring_share >= 0.25:
                synergy_analysis['scoring_role'] = 'Primary Scorer'
            elif scoring_share >= 0.15:
                synergy_analysis['scoring_role'] = 'Secondary Scorer'
            elif scoring_share >= 0.08:
                synergy_analysis['scoring_role'] = 'Role Player'
            else:
                synergy_analysis['scoring_role'] = 'Bench Player'
        
        # Defensive synergy
        if result.get('defensive_contribution'):
            def_share = result['defensive_contribution']
            if def_share >= 0.15:
                synergy_analysis['defensive_role'] = 'Defensive Anchor'
            elif def_share >= 0.08:
                synergy_analysis['defensive_role'] = 'Good Defender'
            elif def_share >= 0.04:
                synergy_analysis['defensive_role'] = 'Adequate Defender'
            else:
                synergy_analysis['defensive_role'] = 'Offensive Focused'
        
        # Win rate synergy
        if result.get('team_win_rate') and result.get('player_win_rate'):
            team_win_rate = result['team_win_rate']
            player_win_rate = result['player_win_rate']
            
            if player_win_rate > team_win_rate + 0.1:
                synergy_analysis['win_impact'] = 'Positive (Player improves team)'
            elif player_win_rate < team_win_rate - 0.1:
                synergy_analysis['win_impact'] = 'Negative (Player hurts team)'
            else:
                synergy_analysis['win_impact'] = 'Neutral (Player matches team)'
        
        return {
            "status": "success",
            "player": player_name,
            "team": team_identifier,
            "season_year": season_year,
            "synergy_data": result,
            "synergy_analysis": synergy_analysis
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def compare_teams_player_impact(
    team_identifiers: List[str],
    season_year: Optional[str] = None,
    comparison_type: str = "scoring",
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Compare how different teams structure their rosters and utilize players.
    
    This tool reveals team philosophies by analyzing player distribution patterns,
    identifying whether teams rely on superstars, have balanced scoring, or
    emphasize specific aspects like defense. Great for team strategy analysis.
    
    Args:
        team_identifiers: List of team identifiers to compare (minimum 2)
                         Can use team abbreviations: ["GSW", "LAL", "BOS"]
                         Or team IDs or names
        season_year: Optional season filter in format "YYYY-YY"
        comparison_type: Focus area for comparison:
                        - "scoring": Scoring distribution and primary scorers
                        - "defensive": Defensive roles and anchors
                        - "comprehensive": Overall player utilization patterns
        project_id: Optional GCP project ID
        
    Returns:
        dict with keys:
        - status: "success" or "error"
        - teams: List of teams compared
        - season_year: Season filter applied (if any)
        - comparison_type: Type of comparison performed
        - team_comparison_data: Statistical data for each team including:
          * unique_players, total_games, average statistics
          * primary_scorers, secondary_scorers (for scoring type)
          * defensive_anchors, good_defenders (for defensive type)
        - utilization_patterns: Team philosophy classification:
          * "Multiple Primary Scorers", "Single Primary Scorer"
          * "Balanced Scoring", "Role Player Heavy"
          * "Multiple Defensive Anchors", "Offensive Focused"
        - count: Number of teams successfully analyzed
        - message: Error message if status is "error"

    Example Usage:
        - "Compare how the Lakers and Warriors use their players"
        - "Which teams have the most balanced scoring?"
        - "How do defensive-focused teams structure their rosters?"
    """
    try:
        if len(team_identifiers) < 2:
            return {"status": "error", "message": "At least 2 teams required for comparison"}
        
        client = _bq_client(project_id)
        season_filter = f"AND p.season_year = '{season_year}'" if season_year else ""
        
        # Build team conditions
        team_conditions = []
        for team_id in team_identifiers:
            if team_id.isdigit():
                team_conditions.append(f"p.teamId = {int(team_id)}")
            elif len(team_id) == 3:
                team_conditions.append(f"UPPER(p.teamTricode) = UPPER('{team_id}')")
            else:
                team_conditions.append(f"LOWER(p.teamSlug) = LOWER('{team_id}')")
        
        team_filter = " OR ".join(team_conditions)
        
        if comparison_type == "scoring":
            query = f"""
            SELECT
              p.teamTricode,
              COUNT(DISTINCT p.personName) AS unique_players,
              COUNT(DISTINCT p.gameId) AS total_games,
              AVG(p.points) AS avg_player_points,
              AVG(t.PTS) AS avg_team_points,
              AVG(p.points / NULLIF(t.PTS, 0)) AS avg_scoring_share,
              MAX(p.points / NULLIF(t.PTS, 0)) AS max_scoring_share,
              COUNT(CASE WHEN p.points / NULLIF(t.PTS, 0) >= 0.25 THEN 1 END) AS primary_scorers,
              COUNT(CASE WHEN p.points / NULLIF(t.PTS, 0) >= 0.15 AND p.points / NULLIF(t.PTS, 0) < 0.25 THEN 1 END) AS secondary_scorers
            FROM `{client.project}.{RAW_TABLE}` p
            JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
              ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
            WHERE ({team_filter})
            {season_filter}
            GROUP BY p.teamTricode
            HAVING total_games >= 20
            ORDER BY avg_scoring_share DESC
            """
        elif comparison_type == "defensive":
            query = f"""
            SELECT
              p.teamTricode,
              COUNT(DISTINCT p.personName) AS unique_players,
              COUNT(DISTINCT p.gameId) AS total_games,
              AVG(p.steals + p.blocks) AS avg_defensive_activity,
              AVG(t.STL + t.BLK) AS avg_team_defensive_activity,
              AVG((p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0)) AS avg_defensive_share,
              MAX((p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0)) AS max_defensive_share,
              COUNT(CASE WHEN (p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0) >= 0.15 THEN 1 END) AS defensive_anchors,
              COUNT(CASE WHEN (p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0) >= 0.08 AND (p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0) < 0.15 THEN 1 END) AS good_defenders
            FROM `{client.project}.{RAW_TABLE}` p
            JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
              ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
            WHERE ({team_filter})
            {season_filter}
            GROUP BY p.teamTricode
            HAVING total_games >= 20
            ORDER BY avg_defensive_share DESC
            """
        else:  # "comprehensive"
            query = f"""
            SELECT
              p.teamTricode,
              COUNT(DISTINCT p.personName) AS unique_players,
              COUNT(DISTINCT p.gameId) AS total_games,
              AVG(p.points) AS avg_player_points,
              AVG(p.assists) AS avg_player_assists,
              AVG(p.reboundsTotal) AS avg_player_rebounds,
              AVG(p.steals + p.blocks) AS avg_player_defensive_activity,
              AVG(t.PTS) AS avg_team_points,
              AVG(t.AST) AS avg_team_assists,
              AVG(t.REB) AS avg_team_rebounds,
              AVG(t.STL + t.BLK) AS avg_team_defensive_activity,
              AVG(p.points / NULLIF(t.PTS, 0)) AS avg_scoring_share,
              AVG(p.assists / NULLIF(t.AST, 0)) AS avg_assist_share,
              AVG(p.reboundsTotal / NULLIF(t.REB, 0)) AS avg_rebound_share,
              AVG((p.steals + p.blocks) / NULLIF(t.STL + t.BLK, 0)) AS avg_defensive_share,
              AVG(t.PLUS_MINUS) AS avg_team_plus_minus
            FROM `{client.project}.{RAW_TABLE}` p
            JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
              ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
            WHERE ({team_filter})
            {season_filter}
            GROUP BY p.teamTricode
            HAVING total_games >= 20
            ORDER BY avg_scoring_share DESC
            """
        
        rows = list(client.query(query).result())
        records = [dict(row) for row in rows]
        
        # Calculate team utilization patterns
        utilization_patterns = {}
        for record in records:
            team = record['teamTricode']
            if comparison_type == "scoring":
                if record.get('primary_scorers', 0) >= 2:
                    utilization_patterns[team] = "Multiple Primary Scorers"
                elif record.get('primary_scorers', 0) == 1:
                    utilization_patterns[team] = "Single Primary Scorer"
                elif record.get('secondary_scorers', 0) >= 3:
                    utilization_patterns[team] = "Balanced Scoring"
                else:
                    utilization_patterns[team] = "Role Player Heavy"
            elif comparison_type == "defensive":
                if record.get('defensive_anchors', 0) >= 2:
                    utilization_patterns[team] = "Multiple Defensive Anchors"
                elif record.get('defensive_anchors', 0) == 1:
                    utilization_patterns[team] = "Single Defensive Anchor"
                elif record.get('good_defenders', 0) >= 3:
                    utilization_patterns[team] = "Balanced Defense"
                else:
                    utilization_patterns[team] = "Offensive Focused"
        
        return {
            "status": "success",
            "teams": team_identifiers,
            "season_year": season_year,
            "comparison_type": comparison_type,
            "team_comparison_data": records,
            "utilization_patterns": utilization_patterns,
            "count": len(records)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def analyze_team_offensive_efficiency_by_player_contribution(
    team_identifier: str,
    season_year: Optional[str] = None,
    project_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze how individual player contributions affect team offensive efficiency.
    
    Args:
        team_identifier: Team ID, tricode, or slug
        season_year: Optional season filter
        project_id: GCP project ID
        
    Returns:
        Team offensive efficiency analysis
    """
    try:
        client = _bq_client(project_id)
        
        # Build team identifier predicate
        if team_identifier.isdigit():
            team_pred = f"p.teamId = {int(team_identifier)}"
        elif len(team_identifier) == 3:
            team_pred = f"UPPER(p.teamTricode) = UPPER('{team_identifier}')"
        else:
            team_pred = f"LOWER(p.teamSlug) = LOWER('{team_identifier}')"
        
        season_filter = f"AND p.season_year = '{season_year}'" if season_year else ""
        
        query = f"""
        SELECT
          p.personName,
          COUNT(DISTINCT p.gameId) AS games_played,
          AVG(p.points) AS avg_points,
          AVG(p.assists) AS avg_assists,
          AVG(t.PTS) AS team_avg_points,
          AVG(t.AST) AS team_avg_assists,
          AVG(p.points / NULLIF(t.PTS, 0)) AS scoring_contribution,
          AVG(p.assists / NULLIF(t.AST, 0)) AS assist_contribution,
          AVG(SAFE_DIVIDE(p.points, 2*(p.fieldGoalsAttempted + 0.44*p.freeThrowsAttempted))) AS player_ts_pct,
          AVG(SAFE_DIVIDE(t.PTS, 2*(t.FGA + 0.44*t.FTA))) AS team_ts_pct,
          AVG(CASE WHEN t.PLUS_MINUS > 0 THEN 1 ELSE 0 END) AS team_win_rate,
          AVG(CASE WHEN p.plusMinusPoints > 0 THEN 1 ELSE 0 END) AS player_win_rate,
          AVG(p.minutes) AS avg_minutes
        FROM `{client.project}.{RAW_TABLE}` p
        JOIN `{client.project}.{TEAM_STATS_TABLE}` t 
          ON p.gameId = t.GAME_ID AND p.teamId = t.TEAM_ID
        WHERE {team_pred}
        {season_filter}
        GROUP BY p.personName
        HAVING games_played >= 10
        ORDER BY scoring_contribution DESC
        """
        
        rows = list(client.query(query).result())
        records = [dict(row) for row in rows]
        
        if not records:
            return {"status": "error", "message": f"No data found for team: {team_identifier}"}
        
        # Calculate efficiency insights
        efficiency_insights = {}
        
        # Find most efficient scorers
        if records:
            max_ts = max(r.get('player_ts_pct', 0) or 0 for r in records)
            most_efficient = [r for r in records if (r.get('player_ts_pct') or 0) == max_ts]
            if most_efficient:
                efficiency_insights['most_efficient_scorer'] = {
                    'player': most_efficient[0]['personName'],
                    'true_shooting_pct': most_efficient[0]['player_ts_pct']
                }
        
        # Find highest contributors
        if records:
            max_contribution = max(r.get('scoring_contribution', 0) or 0 for r in records)
            highest_contributors = [r for r in records if (r.get('scoring_contribution') or 0) == max_contribution]
            if highest_contributors:
                efficiency_insights['highest_scoring_contributor'] = {
                    'player': highest_contributors[0]['personName'],
                    'contribution': highest_contributors[0]['scoring_contribution']
                }
        
        # Calculate team efficiency when players play
        efficiency_analysis = []
        for record in records:
            player_ts = record.get('player_ts_pct', 0) or 0
            team_ts = record.get('team_ts_pct', 0) or 0
            contribution = record.get('scoring_contribution', 0) or 0
            
            if player_ts > team_ts:
                efficiency_analysis.append({
                    'player': record['personName'],
                    'impact': 'Improves team efficiency',
                    'player_ts': player_ts,
                    'team_ts': team_ts,
                    'contribution': contribution
                })
            elif player_ts < team_ts - 0.05:
                efficiency_analysis.append({
                    'player': record['personName'],
                    'impact': 'Reduces team efficiency',
                    'player_ts': player_ts,
                    'team_ts': team_ts,
                    'contribution': contribution
                })
        
        return {
            "status": "success",
            "team": team_identifier,
            "season_year": season_year,
            "player_efficiency_data": records,
            "efficiency_insights": efficiency_insights,
            "efficiency_analysis": efficiency_analysis,
            "total_players_analyzed": len(records)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Expose ADK tools
run_query_tool = FunctionTool(run_query)
get_query_status_tool = FunctionTool(get_query_status)
get_query_results_tool = FunctionTool(get_query_results)

get_player_stats_tool = FunctionTool(get_player_stats)
get_player_stats_by_season_tool = FunctionTool(get_player_stats_by_season)
get_team_stats_tool = FunctionTool(get_team_stats)
get_player_monthly_trends_tool = FunctionTool(get_player_monthly_trends)
analyze_player_efficiency_tool = FunctionTool(analyze_player_efficiency)
analyze_player_defense_tool = FunctionTool(analyze_player_defense)

# New advanced analysis tools
compare_players_advanced_metrics_tool = FunctionTool(compare_players_advanced_metrics)
analyze_team_performance_trends_tool = FunctionTool(analyze_team_performance_trends)
analyze_player_efficiency_deep_dive_tool = FunctionTool(analyze_player_efficiency_deep_dive)
analyze_player_performance_by_game_situation_tool = FunctionTool(analyze_player_performance_by_game_situation)
predict_player_performance_tool = FunctionTool(predict_player_performance)
calculate_advanced_basketball_metrics_tool = FunctionTool(calculate_advanced_basketball_metrics)
analyze_statistical_correlations_tool = FunctionTool(analyze_statistical_correlations)
cluster_players_by_playing_style_tool = FunctionTool(cluster_players_by_playing_style)

# New player-team combined analysis tools
analyze_player_team_impact_tool = FunctionTool(analyze_player_team_impact)
analyze_lineup_effectiveness_tool = FunctionTool(analyze_lineup_effectiveness)
analyze_player_team_synergy_tool = FunctionTool(analyze_player_team_synergy)
compare_teams_player_impact_tool = FunctionTool(compare_teams_player_impact)
analyze_team_offensive_efficiency_by_player_contribution_tool = FunctionTool(analyze_team_offensive_efficiency_by_player_contribution)

