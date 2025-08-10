import base64
import json

import functions_framework
import requests
 
# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
 
  # 取得したWebhook URLを入力する
  WEB_HOOK_URL = ""
  # Cloud Pub/Subから取得したデータを変数に入れる
  question = base64.b64decode(cloud_event.data["message"]["data"]).decode()
 
  requests.post(WEB_HOOK_URL, data=json.dumps({
    #メッセージ
    "text" : "元の質問は：" + question,
    }))
 
  return question