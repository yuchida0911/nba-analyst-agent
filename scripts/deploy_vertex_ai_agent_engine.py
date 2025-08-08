#!/usr/bin/env python3
"""
Deploy the ADK agent to Vertex AI Agent Engine.

Requirements:
- pip install "google-cloud-aiplatform[adk,agent_engines]" cloudpickle
- GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_LOCATION, STAGING_BUCKET env vars set

Usage:
  GOOGLE_CLOUD_PROJECT=your-proj \
  GOOGLE_CLOUD_LOCATION=us-central1 \
  STAGING_BUCKET=gs://your-staging-bucket \
  python scripts/deploy_vertex_ai_agent_engine.py
"""

from __future__ import annotations

import os
import sys
from typing import Optional

import vertexai
from vertexai.preview import reasoning_engines
from vertexai import agent_engines


def _ensure_src_on_path() -> None:
    project_root = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(project_root, os.pardir))
    src_dir = os.path.join(project_root, "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def _get_env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def main() -> None:
    _ensure_src_on_path()

    # Lazy import after sys.path fix
    from analytic_agent.agent import root_agent  # noqa: WPS433

    project_id = _get_env("GOOGLE_CLOUD_PROJECT")
    location = _get_env("GOOGLE_CLOUD_LOCATION", "us-central1")
    staging_bucket = _get_env("STAGING_BUCKET")

    print(f"Initializing Vertex AI: project={project_id}, location={location}, bucket={staging_bucket}")
    vertexai.init(project=project_id, location=location, staging_bucket=staging_bucket)

    print("Wrapping ADK agent for Agent Engine...")
    adk_app = reasoning_engines.AdkApp(agent=root_agent, enable_tracing=True)

    print("Creating remote Agent Engine app (this may take a few minutes)...")
    remote_app = agent_engines.create(
        agent_engine=adk_app,
        # Include local source so the engine can import your modules
        extra_packages=["./src"],
        requirements=[
            "google-cloud-aiplatform[adk,agent_engines]",
        ],
    )

    print("Deployment complete.")
    print(f"Resource name: {remote_app.resource_name}")
    print("You can use this resource name as the app_name when creating sessions.")


if __name__ == "__main__":
    main()

