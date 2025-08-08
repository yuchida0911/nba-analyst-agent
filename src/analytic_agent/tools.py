from __future__ import annotations

"""
NBA Analytics tools for Google ADK.

This module exposes FunctionTool-wrapped functions that the agent can call to:
- Run parameterized BigQuery queries
- Retrieve player and team stats from the `nba_analytics.players_raw` table
- Compute advanced analytics summaries (efficiency and defense) using
  in-repo implementations under `nba_analyst.analytics`

All tools return JSON-serializable dicts with a `status` field.
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
    """Run a BigQuery SQL query.

    Args:
        query: BigQuery SQL query string.
        project_id: Optional GCP project ID. Defaults to env/default.

    Returns:
        dict with keys: status, job_id, project_id, location
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
    """Get status for a BigQuery query job."""
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
    """Fetch results for a BigQuery query job as list of records."""
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
    """Get recent game logs for a player from players_raw.

    Args:
        player_name: Case-insensitive substring match on `personName`.
        season_year: Optional season filter like '2015-16'.
        limit: Max number of rows to return (most recent first).

    Returns:
        dict with basic box score fields and dates.
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
    """Get per-season averages for a player from players_raw."""
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
    """Get recent team aggregated stats per game.

    team_identifier can be a numeric teamId, a 3-letter teamTricode, or a teamSlug.
    """
    try:
        client = _bq_client(project_id)
        # Build identifier predicate
        pred: str
        if team_identifier.isdigit():
            pred = f"teamId = {int(team_identifier)}"
        elif len(team_identifier) == 3:
            pred = f"UPPER(teamTricode) = UPPER('{team_identifier}')"
        else:
            pred = f"LOWER(teamSlug) = LOWER('{team_identifier}')"

        season_filter = f"AND season_year = '{season_year}'" if season_year else ""
        query = f"""
        SELECT
          game_date,
          season_year,
          teamId,
          ANY_VALUE(teamTricode) AS teamTricode,
          SUM(points) AS points,
          SUM(reboundsTotal) AS rebounds,
          SUM(assists) AS assists,
          SUM(steals) AS steals,
          SUM(blocks) AS blocks,
          SUM(turnovers) AS turnovers,
          SUM(fieldGoalsMade) AS fgm,
          SUM(fieldGoalsAttempted) AS fga,
          SUM(threePointersMade) AS tpm,
          SUM(threePointersAttempted) AS tpa,
          SUM(freeThrowsMade) AS ftm,
          SUM(freeThrowsAttempted) AS fta,
          SAFE_DIVIDE(SUM(fieldGoalsMade) + 0.5*SUM(threePointersMade), NULLIF(SUM(fieldGoalsAttempted),0)) AS efg_pct
        FROM `{client.project}.{TEAM_STATS_TABLE}`
        WHERE {pred}
        {season_filter}
        GROUP BY game_date, season_year, teamId
        ORDER BY game_date DESC
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
    """Compute monthly averages and TS% for a player from players_raw."""
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
    """Analyze player efficiency using True Shooting-based trends across recent games."""
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
    """Analyze player defensive strengths using composite defensive impact across recent games."""
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

