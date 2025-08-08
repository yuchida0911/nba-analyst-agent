# Deployment Guide

This repo supports deployment to:

- Vertex AI Agent Engine (managed, recommended for production)
- Cloud Run (serverless container)

## Prerequisites

- gcloud CLI authenticated
  - `gcloud auth login`
  - `gcloud auth application-default login`
- Enable billing on the project
- Python packages for Agent Engine: `pip install "google-cloud-aiplatform[adk,agent_engines]" cloudpickle`
- ADK CLI for Cloud Run: `pip install google-adk`

## Vertex AI Agent Engine

1. Set env vars

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
export GOOGLE_CLOUD_LOCATION="us-central1"
export STAGING_BUCKET="gs://your-staging-bucket"
```

2. Run deploy script

```bash
python scripts/deploy_vertex_ai_agent_engine.py
```

It prints a `resource_name`. Use it for remote sessions.

## Cloud Run (ADK CLI)

1. Set env vars

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
export GOOGLE_CLOUD_LOCATION="us-central1"
```

2. Run deploy

```bash
./scripts/deploy_cloud_run.sh
```

This will deploy `src/analytic_agent` with a simple web UI enabled.
