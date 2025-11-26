import os
import json
import requests
from flask import Flask, request, jsonify, render_template_string
from requests.auth import HTTPBasicAuth
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Airflow API config
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080/api/v1")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")

print("Using base URL:", AIRFLOW_BASE_URL)
print("Using credentials:", AIRFLOW_USERNAME, AIRFLOW_PASSWORD)


def set_airflow_variable(key: str, value: str):
    """Set or update an Airflow variable via API"""
    url = f"{AIRFLOW_BASE_URL}/variables/{key}"
    data = {"value": value}
    auth = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    headers = {"Content-Type": "application/json"}

    response = requests.patch(url, json=data, auth=auth, headers=headers)
    if response.status_code == 404:
        response = requests.post(
            f"{AIRFLOW_BASE_URL}/variables",
            json={"key": key, "value": value},
            auth=auth,
            headers=headers
        )
    response.raise_for_status()
    return response.json()


# HTML template for acknowledgment page
ACK_PAGE_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Alert Acknowledged</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }
        .container {
            background: white;
            padding: 40px;
            border-radius: 10px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            text-align: center;
            max-width: 500px;
        }
        .success-icon {
            font-size: 64px;
            margin-bottom: 20px;
        }
        h1 {
            color: #2d3748;
            margin-bottom: 10px;
        }
        p {
            color: #4a5568;
            line-height: 1.6;
        }
        .details {
            background: #f7fafc;
            padding: 15px;
            border-radius: 5px;
            margin-top: 20px;
            text-align: left;
        }
        .details strong {
            color: #2d3748;
        }
        .error {
            color: #e53e3e;
        }
    </style>
</head>
<body>
    <div class="container">
        {% if success %}
        <div class="success-icon">✅</div>
        <h1>Alert Acknowledged!</h1>
        <p>The cascade calling has been stopped.</p>
        <div class="details">
            <p><strong>DAG:</strong> {{ dag_id }}</p>
            <p><strong>Task:</strong> {{ task_id }}</p>
            <p><strong>Timestamp:</strong> {{ timestamp }}</p>
        </div>
        <p style="margin-top: 20px; color: #718096; font-size: 14px;">
            You can close this window now.
        </p>
        {% else %}
        <div class="success-icon error">❌</div>
        <h1>Acknowledgment Failed</h1>
        <p class="error">{{ error }}</p>
        {% endif %}
    </div>
</body>
</html>
"""


@app.route("/api/acknowledge", methods=["POST"])
def acknowledge_post():
    """
    POST endpoint for MessageCard HttpPOST action
    """
    try:
        req = request.get_json()
        dag_id = req.get("dag_id")
        task_id = req.get("task_id", "unknown")

        if not dag_id:
            return jsonify({
                "status": "error",
                "message": "dag_id is required"
            }), 400

        var_key = f"twilio_alert_enabled_{dag_id}"

        # Set Airflow variable to 'false' to pause alerts
        set_airflow_variable(var_key, "false")

        logger.info(f"Alert acknowledged for DAG '{dag_id}', Task '{task_id}'")

        return jsonify({
            "status": "success",
            "message": f"Alerts paused for DAG '{dag_id}' based on acknowledgment from task '{task_id}'.",
            "dag_id": dag_id,
            "task_id": task_id
        })
    except Exception as e:
        logger.error(f"Error acknowledging alert: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route("/api/acknowledge", methods=["GET"])
def acknowledge_get():
    """
    GET endpoint for Adaptive Card link acknowledgment
    Opens a nice web page showing success
    """
    try:
        dag_id = request.args.get("dag_id")
        task_id = request.args.get("task_id", "unknown")

        if not dag_id:
            return render_template_string(
                ACK_PAGE_TEMPLATE,
                success=False,
                error="Missing dag_id parameter"
            ), 400

        var_key = f"twilio_alert_enabled_{dag_id}"

        # Set Airflow variable to 'false' to pause alerts
        set_airflow_variable(var_key, "false")

        logger.info(f"Alert acknowledged via GET for DAG '{dag_id}', Task '{task_id}'")

        from datetime import datetime
        return render_template_string(
            ACK_PAGE_TEMPLATE,
            success=True,
            dag_id=dag_id,
            task_id=task_id,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    except Exception as e:
        logger.error(f"Error acknowledging alert via GET: {e}")
        return render_template_string(
            ACK_PAGE_TEMPLATE,
            success=False,
            error=str(e)
        ), 500


@app.route("/api/acknowledge/status/<dag_id>", methods=["GET"])
def get_acknowledgment_status(dag_id):
    """
    Check if alerts are enabled/disabled for a DAG
    """
    try:
        var_key = f"twilio_alert_enabled_{dag_id}"
        url = f"{AIRFLOW_BASE_URL}/variables/{var_key}"
        auth = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
        
        response = requests.get(url, auth=auth)
        
        if response.status_code == 404:
            # Variable doesn't exist, alerts are enabled by default
            return jsonify({
                "dag_id": dag_id,
                "alerts_enabled": True,
                "acknowledged": False
            }), 200
        
        response.raise_for_status()
        var_data = response.json()
        alerts_enabled = var_data.get("value", "true").lower() == "true"
        
        return jsonify({
            "dag_id": dag_id,
            "alerts_enabled": alerts_enabled,
            "acknowledged": not alerts_enabled
        }), 200
    
    except Exception as e:
        logger.error(f"Error getting acknowledgment status: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/api/acknowledge/reset/<dag_id>", methods=["POST"])
def reset_acknowledgment(dag_id):
    """
    Reset acknowledgment (re-enable alerts) for testing
    """
    try:
        var_key = f"twilio_alert_enabled_{dag_id}"
        set_airflow_variable(var_key, "true")
        
        logger.info(f"Alerts re-enabled for DAG '{dag_id}'")
        
        return jsonify({
            "success": True,
            "message": f"Alerts re-enabled for DAG '{dag_id}'"
        }), 200
    
    except Exception as e:
        logger.error(f"Error resetting acknowledgment: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "Airflow Alert Acknowledgment API"
    }), 200


@app.route("/", methods=["GET"])
def index():
    """Simple index page"""
    return """
    <html>
    <head><title>Airflow Alert ACK Service</title></head>
    <body style="font-family: sans-serif; padding: 40px;">
        <h1>🚨 Airflow Alert Acknowledgment Service</h1>
        <p>This service handles alert acknowledgments from Microsoft Teams.</p>
        <h3>Endpoints:</h3>
        <ul>
            <li><strong>POST</strong> /api/acknowledge - Acknowledge via POST</li>
            <li><strong>GET</strong> /api/acknowledge?dag_id=X&task_id=Y - Acknowledge via link</li>
            <li><strong>GET</strong> /api/acknowledge/status/&lt;dag_id&gt; - Check status</li>
            <li><strong>POST</strong> /api/acknowledge/reset/&lt;dag_id&gt; - Reset acknowledgment</li>
            <li><strong>GET</strong> /health - Health check</li>
        </ul>
    </body>
    </html>
    """


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
