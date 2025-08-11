#!/usr/bin/env python3
"""
Deploy the ADK agent to Vertex AI Agent Engine.

Usage:
  GOOGLE_CLOUD_PROJECT=your-proj \
  GOOGLE_CLOUD_LOCATION=us-central1 \
  STAGING_BUCKET=gs://your-staging-bucket \
  python deployment/deploy.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import vertexai
from vertexai import agent_engines
from vertexai.preview.reasoning_engines import AdkApp
from nba_analyst_agent.agent import root_agent  # noqa: E402

# Ensure project root is on sys.path so top-level packages can be imported
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "yuchida-dev")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
STAGING_BUCKET = os.getenv("STAGING_BUCKET", "gs://nba-analytics-agent-staging")

WHL_FILE = "nba_analyst_agent-0.1-py3-none-any.whl"

def main() -> None:
    # Initialize Vertex AI SDK
    vertexai.init(project=PROJECT_ID, location=LOCATION, staging_bucket=STAGING_BUCKET)

    app = AdkApp(agent=root_agent, enable_tracing=True)
    
    remote_app = agent_engines.create(
        agent_engine=app,
        requirements=[
            WHL_FILE,
        ],
        display_name="NBA Analytics Agent",
        description="An agent that can answer questions about NBA data",
        extra_packages=[WHL_FILE],
    )

    print(remote_app.resource_name)


if __name__ == "__main__":
    main()