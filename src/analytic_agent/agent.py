import os
import google.auth
from pathlib import Path
from google.adk.agents import Agent
from google.adk.tools.bigquery import BigQueryToolset
from google.adk.tools.bigquery import BigQueryCredentialsConfig
from google.adk.tools.base_tool import BaseTool
from google.adk.tools.tool_context import ToolContext
from .tools import (
    get_player_stats_tool,
    get_player_stats_by_season_tool,
    get_team_stats_tool,
    get_player_monthly_trends_tool,
    analyze_player_efficiency_tool,
    analyze_player_defense_tool,
)

# CREDENTIALS_TYPE = AuthCredentialTypes.SERVICE_ACCOUNT

# if CREDENTIALS_TYPE == AuthCredentialTypes.OAUTH2:
#   # Initiaze the tools to do interactive OAuth
#   credentials_config = BigQueryCredentialsConfig(
#       client_id=os.getenv("OAUTH_CLIENT_ID"),
#       client_secret=os.getenv("OAUTH_CLIENT_SECRET"),
#   )
# elif CREDENTIALS_TYPE == AuthCredentialTypes.SERVICE_ACCOUNT:
#   # Initialize the tools to use the credentials in the service account key.
#   creds, _ = google.auth.load_credentials_from_file("service_account_key.json")
#   credentials_config = BigQueryCredentialsConfig(credentials=creds)
# else:
#   # Initialize the tools to use the application default credentials.
#   application_default_credentials, _ = google.auth.default()
#   credentials_config = BigQueryCredentialsConfig(credentials=application_default_credentials)

def _load_bq_credentials():
    """Resolve and load service account credentials robustly.

    Resolution order:
    - GOOGLE_APPLICATION_CREDENTIALS (absolute or relative)
    - <project_root>/secrets/yuchida-dev-d61ebd48c01e.json
    - CWD/secrets/yuchida-dev-d61ebd48c01e.json
    - Fallback to Application Default Credentials
    """
    # Candidate 1: from env
    env_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    candidates = []
    if env_path:
        candidates.append(Path(env_path))

    # Candidate 2: project_root/secrets/<file>
    project_root = Path(__file__).resolve().parents[2]
    candidates.append(project_root / "secrets" / "yuchida-dev-d61ebd48c01e.json")

    # Candidate 3: CWD/secrets/<file>
    candidates.append(Path.cwd() / "secrets" / "yuchida-dev-d61ebd48c01e.json")

    for p in candidates:
        try:
            if p and p.exists():
                creds, _ = google.auth.load_credentials_from_file(str(p))
                return creds
        except Exception:
            pass

    # Fallback to ADC
    creds, _ = google.auth.default()
    return creds


# Initialize the tools to use resolved credentials
credentials_config = BigQueryCredentialsConfig(credentials=_load_bq_credentials())

_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "yuchida-dev")
_DEFAULT_DATASET = os.getenv("NBA_ANALYTICS_DATASET", "nba_analytics")

# Initialize ADK's native BigQuery toolset
_bq_toolset = BigQueryToolset(credentials_config=credentials_config)

# Ensure BigQuery calls target the correct project/dataset
def _before_tool_callback(tool: BaseTool, args: dict, tool_context: ToolContext):
    tool_name = getattr(tool, "name", "").lower()
    if "execute_sql" in tool_name:
        # Force project_id to default if missing or incorrect
        if not args.get("project_id") or args.get("project_id") != _PROJECT_ID:
            args["project_id"] = _PROJECT_ID
        # Nudge queries toward fully-qualified table names when obvious
        q = args.get("query")
        if isinstance(q, str):
            # If user references unqualified players_raw, qualify it
            # Avoid heavy rewrites; only simple common cases
            if "`players_raw`" in q:
                args["query"] = q.replace(
                    "`players_raw`",
                    f"`{_PROJECT_ID}.{_DEFAULT_DATASET}.players_raw`",
                )
            elif " nba_analytics.players_raw" in q and f"{_PROJECT_ID}.nba_analytics" not in q:
                args["query"] = q.replace(
                    " nba_analytics.players_raw",
                    f" `{_PROJECT_ID}.nba_analytics.players_raw`",
                )

root_agent = Agent(
    name="nba_analyst_agent",
    model="gemini-2.0-flash",
    description="Agent that analyzes NBA data using BigQuery and advanced metrics.",
    instruction="""
    You are an expert NBA analyst. Use the tools to:
    - Retrieve player game logs, per-season summaries, team game summaries
    - Analyze efficiency (True Shooting, trends, consistency) and defense (steal/block rates, impact)
    - Compute monthly trends and recent performance
    - Run custom BigQuery when needed

    Be precise and cite metrics clearly. Prefer the specialized analysis tools first
    (analyze_player_efficiency, analyze_player_defense) and fall back to raw stats tools
    when required. Use the native BigQuery tools for SQL execution and metadata.
    If a tool returns status=error, recover or ask for clarification.
    """,
    tools=[
        _bq_toolset,
        # Stats retrieval
        get_player_stats_tool,
        get_player_stats_by_season_tool,
        get_team_stats_tool,
        get_player_monthly_trends_tool,
        # Analytics
        analyze_player_efficiency_tool,
        analyze_player_defense_tool,
    ],
    before_tool_callback=_before_tool_callback,
)