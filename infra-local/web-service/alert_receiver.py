#!/usr/bin/env python
"""Simple webhook receiver for Grafana alerts"""

from flask import Flask, request, jsonify
import json
from datetime import datetime

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    alert = request.json
    print(f"\n ALERT RECEIVED at {datetime.now()}")
    print(f"Title: {alert.get('title', 'No title')}")
    print(f"Message: {alert.get('message', 'No message')}")
    print(f"State: {alert.get('state', 'No state')}")
    print("Full alert:", json.dumps(alert, indent=2))
    
    # Здесь можно добавить отправку в Telegram, Slack, Email и т.д.
    # Для простоты просто логируем в консоль
    
    return jsonify({"status": "received"}), 200

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok"})

if __name__ == '__main__':
    print("Starting alert webhook receiver on http://localhost:8089")
    app.run(host='0.0.0.0', port=8089, debug=True)