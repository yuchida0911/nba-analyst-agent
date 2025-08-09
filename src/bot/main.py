from cgitb import text
import json
import logging
import os
import re
from typing import Union

import functions_framework
import google.cloud.logging
from google.cloud import secretmanager
from box import Box
from flask import Request
from slack_bolt import App
from slack_bolt.adapter.google_cloud_functions import SlackRequestHandler

# Google Cloud Logging クライアント ライブラリを設定
logging_client = google.cloud.logging.Client()
logging_client.setup_logging(log_level=logging.DEBUG)

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """Retrieve a secret from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Get Slack token from Secret Manager
PROJECT_ID = os.environ.get("GCP_PROJECT", "yuchida-dev")

try:
    slack_token = get_secret(PROJECT_ID, "SLACK_BOT_TOKEN")
    signing_secret = get_secret(PROJECT_ID, "SLACK_SIGNING_SECRET")
    app = App(token=slack_token, signing_secret=signing_secret, process_before_response=True)
    
    # Register event handlers after app initialization
    @app.event("app_mention")
    def handle_app_mention(body: dict, say, client):
        """ Handle app mention event """
        logging.info(f"Received event: {body}")
        box = Box(body)
        user = box.event.user
        thread_ts = box.event.thread_ts or box.event.ts
        only_text = re.sub(r"<@[A-Z0-9]+>", "", box.event.text)

        # Add reaction to the message (like Cursor)
        try:
            client.reactions_add(
                channel=box.event.channel,
                timestamp=thread_ts,
                name="hourglass"
            )
        except Exception as e:
            logging.error(f"Error adding reaction: {e}")


        logging.info(f"Only text: {only_text}")
        say(text=f"Processing...", thread_ts=thread_ts)

    # 'hello' を含むメッセージをリッスンします
    @app.message("hello")
    def message_hello(message, say):
        # イベントがトリガーされたチャンネルへ say() でメッセージを送信します
        say(
            text=f"Hey there <@{message['user']}>!"
        )
    
    handler = SlackRequestHandler(app)
    logging.info("Slack app initialized successfully")
except Exception as e:
    logging.error(f"Failed to initialize Slack app: {str(e)}")
    # Create a dummy app for error handling
    app = App(token="dummy", process_before_response=True)
    handler = SlackRequestHandler(app)

# Google Cloud Functions のエントリーポイント
@functions_framework.http
def slack_bot(request: Request):
    """ Handle HTTP request from Slack 
    
    Args:
        request: The HTTP request from Slack
        
    Returns:
        SlackRequestHandler.handle() (SlackRequestHandler connection)
    """
    logging.info(f"slack_bot: Request: {request}")

    return SlackRequestHandler(app).handle(request)
