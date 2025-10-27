"""
Prefect API Integration for Heart Disease Prediction
Integrates Flask API with Prefect flows for real-time pipeline monitoring
"""

import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, Any, List
from flask import Flask, jsonify, request
from prefect import flow, task, get_run_logger
import joblib
import glob


# Initialize Flask app
app = Flask(__name__)

# Global variables to store pipeline status
pipeline_status = {
    "data_pipeline": {"status": "not_run", "last_run": None, "summary": None},
    "ml_pipeline": {"status": "not_run", "last_run": None, "summary": None}
}


@task(name="get_pipeline_status", retries=1)
def get_pipeline_status() -> Dict[str, Any]:
    """
    Get current status of all pipelines
    """
    logger = get_run_logger()
    logger.info("Retrieving pipeline status")
    
    return pipeline_status


@task(name="get_dataset_info", retries=1)
def get_dataset_info(processed_file: str = "heart_processed.csv") -> Dict[str, Any]:
    """
    Get information about the processed dataset
    """
    logger = get_run_logger()
    
    try:
        if os.path.exists(processed_file):
            df = pd.read_csv(processed_file)
            dataset_info = {
                "file_exists": True,
                "rows": len(df),
                "columns": len(df.columns),
                "column_names": list(df.columns),
                "file_size": os.path.getsize(processed_file),
                "last_modified": datetime.fromtimestamp(os.path.getmtime(processed_file)).isoformat()
            }
        else:
            dataset_info = {
                "file_exists": False,
                "rows": 0,
                "columns": 0,
                "column_names": [],
                "file_size": 0,
                "last_modified": None
            }
        
        logger.info(f"Dataset info retrieved: {dataset_info}")
        return dataset_info
        
    except Exception as e:
        logger.error(f"Error getting dataset info: {str(e)}")
        return {"error": str(e)}


@task(name="get_model_info", retries=1)
def get_model_info(models_dir: str = "models") -> Dict[str, Any]:
    """
    Get information about trained models
    """
    logger = get_run_logger()
    
    try:
        model_info = {
            "models_dir_exists": os.path.exists(models_dir),
            "available_models": [],
            "model_files": [],
            "metrics_files": []
        }
        
        if os.path.exists(models_dir):
            # Get all model files
            model_files = glob.glob(os.path.join(models_dir, "*_model.pkl"))
            metrics_files = glob.glob(os.path.join(models_dir, "*_metrics.json"))
            
            model_info["model_files"] = [os.path.basename(f) for f in model_files]
            model_info["metrics_files"] = [os.path.basename(f) for f in metrics_files]
            
            # Load metrics for each model
            for metrics_file in metrics_files:
                try:
                    with open(metrics_file, 'r') as f:
                        metrics_data = json.load(f)
                        model_info["available_models"].append({
                            "name": metrics_data.get("model_name", "Unknown"),
                            "timestamp": metrics_data.get("timestamp", "Unknown"),
                            "accuracy": metrics_data.get("metrics", {}).get("accuracy", 0),
                            "f1_score": metrics_data.get("metrics", {}).get("f1_score", 0)
                        })
                except Exception as e:
                    logger.warning(f"Could not load metrics from {metrics_file}: {str(e)}")
        
        logger.info(f"Model info retrieved: {len(model_info['available_models'])} models found")
        return model_info
        
    except Exception as e:
        logger.error(f"Error getting model info: {str(e)}")
        return {"error": str(e)}


@task(name="get_log_files", retries=1)
def get_log_files() -> Dict[str, Any]:
    """
    Get information about log files
    """
    logger = get_run_logger()
    
    log_files = {
        "data_pipeline_log": "data_pipeline.log",
        "ml_pipeline_log": "ml_pipeline.log"
    }
    
    log_info = {}
    
    for log_name, log_file in log_files.items():
        if os.path.exists(log_file):
            try:
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                    log_info[log_name] = {
                        "exists": True,
                        "size": os.path.getsize(log_file),
                        "lines": len(lines),
                        "last_modified": datetime.fromtimestamp(os.path.getmtime(log_file)).isoformat(),
                        "last_10_lines": lines[-10:] if lines else []
                    }
            except Exception as e:
                log_info[log_name] = {"exists": True, "error": str(e)}
        else:
            log_info[log_name] = {"exists": False}
    
    logger.info(f"Log files info retrieved: {len(log_info)} log files checked")
    return log_info


@flow(
    name="api-status-pipeline",
    description="Pipeline to gather all status information for API endpoints"
)
def api_status_pipeline() -> Dict[str, Any]:
    """
    Main flow to gather all status information for API
    """
    logger = get_run_logger()
    logger.info("Starting API Status Pipeline")
    
    # Get all status information
    pipeline_status_info = get_pipeline_status()
    dataset_info = get_dataset_info()
    model_info = get_model_info()
    log_info = get_log_files()
    
    # Combine all information
    api_status = {
        "timestamp": datetime.now().isoformat(),
        "pipeline_status": pipeline_status_info,
        "dataset_info": dataset_info,
        "model_info": model_info,
        "log_info": log_info,
        "system_status": "running"
    }
    
    logger.info("API Status Pipeline completed")
    return api_status


# Flask API Routes
@app.route('/app/details', methods=['GET'])
def get_app_details():
    """
    Main API endpoint to get application details
    """
    try:
        # Run the status pipeline
        status_info = api_status_pipeline()
        
        # Format response for API
        response = {
            "dataset_rows": status_info["dataset_info"].get("rows", 0),
            "dataset_columns": status_info["dataset_info"].get("columns", 0),
            "column_names": status_info["dataset_info"].get("column_names", []),
            "models_trained": [model["name"] for model in status_info["model_info"].get("available_models", [])],
            "metrics": {
                model["name"].lower().replace(" ", "_") + "_accuracy": model.get("accuracy", 0)
                for model in status_info["model_info"].get("available_models", [])
            },
            "pipeline_status": status_info["pipeline_status"],
            "last_updated": status_info["timestamp"],
            "system_status": status_info["system_status"]
        }
        
        return jsonify(response)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/pipeline/status', methods=['GET'])
def get_pipeline_status_endpoint():
    """
    Get detailed pipeline status
    """
    try:
        status_info = api_status_pipeline()
        return jsonify(status_info)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/models/info', methods=['GET'])
def get_models_info():
    """
    Get detailed model information
    """
    try:
        model_info = get_model_info()
        return jsonify(model_info)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/dataset/info', methods=['GET'])
def get_dataset_info_endpoint():
    """
    Get detailed dataset information
    """
    try:
        dataset_info = get_dataset_info()
        return jsonify(dataset_info)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/logs/recent', methods=['GET'])
def get_recent_logs():
    """
    Get recent log entries
    """
    try:
        log_info = get_log_files()
        return jsonify(log_info)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint
    """
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Heart Disease Prediction API"
    })


# Function to update pipeline status (called from other pipelines)
def update_pipeline_status(pipeline_name: str, status: str, summary: Dict[str, Any] = None):
    """
    Update the global pipeline status
    """
    global pipeline_status
    pipeline_status[pipeline_name] = {
        "status": status,
        "last_run": datetime.now().isoformat(),
        "summary": summary
    }


if __name__ == '__main__':
    print("Starting Heart Disease Prediction API with Prefect Integration")
    print("Available endpoints:")
    print("  GET /app/details - Main application details")
    print("  GET /pipeline/status - Detailed pipeline status")
    print("  GET /models/info - Model information")
    print("  GET /dataset/info - Dataset information")
    print("  GET /logs/recent - Recent log entries")
    print("  GET /health - Health check")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
