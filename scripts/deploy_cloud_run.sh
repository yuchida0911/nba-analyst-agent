#!/usr/bin/env bash
set -euo pipefail

# Deploy the ADK agent to Cloud Run using the ADK CLI.
# Prereqs:
#   - adk installed (pip install google-adk)
#   - gcloud authenticated (gcloud auth login)
#   - env vars set: GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_LOCATION
# Optional:
#   - SERVICE_NAME, APP_NAME

if [[ -z "${GOOGLE_CLOUD_PROJECT:-}" ]]; then
  echo "GOOGLE_CLOUD_PROJECT is required" >&2
  exit 1
fi
if [[ -z "${GOOGLE_CLOUD_LOCATION:-}" ]]; then
  echo "GOOGLE_CLOUD_LOCATION is required" >&2
  exit 1
fi

AGENT_PATH="src/analytic_agent"
SERVICE_NAME=${SERVICE_NAME:-nba-analyst-agent}
APP_NAME=${APP_NAME:-nba-analyst-app}

echo "Deploying to Cloud Run: project=$GOOGLE_CLOUD_PROJECT region=$GOOGLE_CLOUD_LOCATION service=$SERVICE_NAME app=$APP_NAME"

adk deploy cloud_run \
  --project="$GOOGLE_CLOUD_PROJECT" \
  --region="$GOOGLE_CLOUD_LOCATION" \
  --service_name="$SERVICE_NAME" \
  --app_name="$APP_NAME" \
  --with_ui \
  "$AGENT_PATH"

echo "Deployment triggered. Check Cloud Run services in the console."

