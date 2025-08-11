import base64
import json
import logging
import os
from typing import Dict, Any

import functions_framework
import requests
from google.cloud import secretmanager
from google.adk.sessions import VertexAiSessionService
import asyncio
import vertexai
from vertexai import agent_engines


PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION")
BUCKET = os.getenv("GOOGLE_CLOUD_STORAGE_BUCKET")
AGENT_ID = os.getenv("AGENT_ID")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """Retrieve a secret from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Get Slack bot token from Secret Manager
try:
    SLACK_BOT_TOKEN = get_secret(PROJECT_ID, "SLACK_BOT_TOKEN")
except Exception as e:
    logger.error(f"Failed to get Slack token: {e}")
    SLACK_BOT_TOKEN = None

@functions_framework.cloud_event
def process_nba_analytics(cloud_event):
    """Process NBA analytics request from PubSub and respond to Slack thread."""
    
    try:
        vertexai.init(
            project=PROJECT_ID,
            location=LOCATION,
            staging_bucket=f"gs://{BUCKET}",
        )
        # Initialize Vertex AI Agent Engine
        session_service = VertexAiSessionService(PROJECT_ID, LOCATION)
        session = asyncio.run(session_service.create_session(
            app_name=AGENT_ID,
            user_id="slack_bot")
        )
        # Get the message data (instructions)
        message_data = cloud_event.data["message"]["data"]
        instructions = base64.b64decode(message_data).decode("utf-8")
        
        # Get message attributes (Slack metadata)
        attributes = cloud_event.data["message"].get("attributes", {})
        thread_ts = attributes.get("thread_ts")
        channel = attributes.get("channel")
        user = attributes.get("user")
        message_ts = attributes.get("message_ts")
        event_type = attributes.get("event_type")
        
        logger.info(f"Processing instructions: {instructions}")
        logger.info(f"Slack metadata - Channel: {channel}, Thread: {thread_ts}, User: {user}")
        # Send user query to Agent Engine
        for event in session_service.stream_query(
            user_id="slack_bot",
            session_id=session.id,
            message=instructions
        ):
            if "content" in event:
                if "parts" in event["content"]:
                    parts = event["content"]["parts"]
                    for part in parts:
                        if "text" in part:
                            text_part = part["text"]
                            if SLACK_BOT_TOKEN and channel and thread_ts:
                                send_slack_response(channel, text_part, thread_ts, user)
                            else:
                                logger.error("Missing Slack token or thread information - cannot send response")
        
    except Exception as e:
        logger.error(f"Error processing PubSub message: {e}")
        return {"status": "error", "error": str(e)}

def send_slack_response(channel: str, text: str, thread_ts: str, user: str):
    """Send response back to Slack thread."""
    
    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "channel": channel,
        "text": text,
        "thread_ts": thread_ts,  # This makes it reply in thread
        "unfurl_links": False,
        "unfurl_media": False
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        result = response.json()
        if result.get("ok"):
            logger.info("Successfully sent Slack response")
            
            # Remove the hourglass reaction and add checkmark
            remove_reaction(channel, thread_ts, "hourglass")
            add_reaction(channel, thread_ts, "white_check_mark")
            
        else:
            logger.error(f"Slack API error: {result.get('error')}")
            
    except Exception as e:
        logger.error(f"Error sending Slack message: {e}")

def add_reaction(channel: str, timestamp: str, emoji: str):
    """Add reaction to a Slack message."""
    url = "https://slack.com/api/reactions.add"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "channel": channel,
        "timestamp": timestamp,
        "name": emoji
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.json().get("ok"):
            logger.info(f"Added reaction: {emoji}")
        else:
            logger.error(f"Failed to add reaction: {response.json().get('error')}")
    except Exception as e:
        logger.error(f"Error adding reaction: {e}")

def remove_reaction(channel: str, timestamp: str, emoji: str):
    """Remove reaction from a Slack message."""
    url = "https://slack.com/api/reactions.remove"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "channel": channel,
        "timestamp": timestamp,
        "name": emoji
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.json().get("ok"):
            logger.info(f"Removed reaction: {emoji}")
        else:
            logger.error(f"Failed to remove reaction: {response.json().get('error')}")
    except Exception as e:
        logger.error(f"Error removing reaction: {e}")