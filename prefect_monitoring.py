"""
Prefect Monitoring and Logging Configuration
Advanced monitoring, alerting, and logging capabilities for the heart disease ML pipeline
"""

import logging
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect.client.schemas import FlowRun
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


# Configure advanced logging
def setup_advanced_logging():
    """
    Setup advanced logging configuration with multiple handlers
    """
    
    # Create logs directory
    os.makedirs("logs", exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/heart_disease_pipeline.log'),
            logging.StreamHandler()
        ]
    )
    
    # Create specific loggers for different components
    data_logger = logging.getLogger('data_pipeline')
    ml_logger = logging.getLogger('ml_pipeline')
    api_logger = logging.getLogger('api')
    monitor_logger = logging.getLogger('monitoring')
    
    # Add file handlers for each component
    for logger_name, logger in [('data', data_logger), ('ml', ml_logger), ('api', api_logger), ('monitor', monitor_logger)]:
        handler = logging.FileHandler(f'logs/{logger_name}_pipeline.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return data_logger, ml_logger, api_logger, monitor_logger


@task(name="collect_pipeline_metrics", retries=1)
def collect_pipeline_metrics() -> Dict[str, Any]:
    """
    Collect comprehensive metrics from all pipeline components
    """
    logger = get_run_logger()
    logger.info("Collecting pipeline metrics")
    
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "data_pipeline": {},
        "ml_pipeline": {},
        "api_status": {},
        "system_metrics": {}
    }
    
    # Data pipeline metrics
    if os.path.exists("heart_processed.csv"):
        df = pd.read_csv("heart_processed.csv")
        metrics["data_pipeline"] = {
            "processed_rows": len(df),
            "processed_columns": len(df.columns),
            "file_size_mb": os.path.getsize("heart_processed.csv") / (1024 * 1024),
            "last_processed": datetime.fromtimestamp(os.path.getmtime("heart_processed.csv")).isoformat()
        }
    
    # ML pipeline metrics
    models_dir = "models"
    if os.path.exists(models_dir):
        model_files = [f for f in os.listdir(models_dir) if f.endswith('.pkl')]
        metrics_files = [f for f in os.listdir(models_dir) if f.endswith('.json')]
        
        metrics["ml_pipeline"] = {
            "trained_models": len(model_files),
            "model_files": model_files,
            "metrics_files": metrics_files
        }
        
        # Load latest metrics
        for metrics_file in metrics_files:
            try:
                with open(os.path.join(models_dir, metrics_file), 'r') as f:
                    model_metrics = json.load(f)
                    model_name = model_metrics.get("model_name", "Unknown")
                    metrics["ml_pipeline"][f"{model_name.lower().replace(' ', '_')}_accuracy"] = model_metrics.get("metrics", {}).get("accuracy", 0)
            except Exception as e:
                logger.warning(f"Could not load metrics from {metrics_file}: {str(e)}")
    
    # API status metrics
    metrics["api_status"] = {
        "api_running": True,  # This would be checked via health endpoint
        "endpoints_available": ["/app/details", "/pipeline/status", "/models/info", "/health"]
    }
    
    # System metrics
    metrics["system_metrics"] = {
        "log_files_count": len([f for f in os.listdir("logs") if f.endswith('.log')]) if os.path.exists("logs") else 0,
        "disk_usage_mb": sum(os.path.getsize(os.path.join(dirpath, filename)) 
                           for dirpath, dirnames, filenames in os.walk('.') 
                           for filename in filenames) / (1024 * 1024)
    }
    
    logger.info(f"Metrics collected: {len(metrics)} categories")
    return metrics


@task(name="generate_performance_report", retries=1)
def generate_performance_report(metrics: Dict[str, Any]) -> str:
    """
    Generate a comprehensive performance report
    """
    logger = get_run_logger()
    logger.info("Generating performance report")
    
    report_path = f"reports/performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    os.makedirs("reports", exist_ok=True)
    
    # Add analysis to metrics
    report_data = {
        "report_generated": datetime.now().isoformat(),
        "metrics": metrics,
        "analysis": {
            "data_pipeline_status": "healthy" if metrics["data_pipeline"] else "no_data",
            "ml_pipeline_status": "healthy" if metrics["ml_pipeline"].get("trained_models", 0) > 0 else "no_models",
            "overall_status": "operational"
        },
        "recommendations": []
    }
    
    # Add recommendations based on metrics
    if not metrics["data_pipeline"]:
        report_data["recommendations"].append("Run data pipeline to process raw data")
    
    if not metrics["ml_pipeline"].get("trained_models", 0):
        report_data["recommendations"].append("Train ML models to enable predictions")
    
    if metrics["system_metrics"]["disk_usage_mb"] > 1000:  # More than 1GB
        report_data["recommendations"].append("Consider cleaning up old log files and models")
    
    # Save report
    with open(report_path, 'w') as f:
        json.dump(report_data, f, indent=2)
    
    logger.info(f"Performance report saved to: {report_path}")
    return report_path


@task(name="create_monitoring_dashboard", retries=1)
def create_monitoring_dashboard(metrics: Dict[str, Any]) -> str:
    """
    Create a visual monitoring dashboard
    """
    logger = get_run_logger()
    logger.info("Creating monitoring dashboard")
    
    # Create dashboard directory
    os.makedirs("dashboard", exist_ok=True)
    
    # Set up the plot style
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Heart Disease ML Pipeline Monitoring Dashboard', fontsize=16)
    
    # 1. Data Pipeline Status
    ax1 = axes[0, 0]
    data_status = "Active" if metrics["data_pipeline"] else "Inactive"
    colors = ['green' if data_status == "Active" else 'red']
    ax1.pie([1], labels=[data_status], colors=colors, autopct='%1.0f%%')
    ax1.set_title('Data Pipeline Status')
    
    # 2. ML Models Status
    ax2 = axes[0, 1]
    model_count = metrics["ml_pipeline"].get("trained_models", 0)
    ax2.bar(['Trained Models'], [model_count], color='blue')
    ax2.set_title('ML Models Status')
    ax2.set_ylabel('Number of Models')
    
    # 3. System Metrics
    ax3 = axes[1, 0]
    disk_usage = metrics["system_metrics"]["disk_usage_mb"]
    log_files = metrics["system_metrics"]["log_files_count"]
    
    categories = ['Disk Usage (MB)', 'Log Files']
    values = [disk_usage, log_files]
    ax3.bar(categories, values, color=['orange', 'purple'])
    ax3.set_title('System Metrics')
    ax3.set_ylabel('Count/Size')
    
    # 4. Pipeline Timeline (simplified)
    ax4 = axes[1, 1]
    timeline_data = {
        'Data Processing': metrics["data_pipeline"].get("processed_rows", 0),
        'ML Training': model_count,
        'API Endpoints': len(metrics["api_status"]["endpoints_available"])
    }
    
    ax4.bar(timeline_data.keys(), timeline_data.values(), color=['green', 'blue', 'red'])
    ax4.set_title('Pipeline Components')
    ax4.set_ylabel('Count')
    ax4.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    # Save dashboard
    dashboard_path = f"dashboard/monitoring_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(dashboard_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Monitoring dashboard saved to: {dashboard_path}")
    return dashboard_path


@task(name="check_pipeline_health", retries=1)
def check_pipeline_health() -> Dict[str, Any]:
    """
    Comprehensive health check for all pipeline components
    """
    logger = get_run_logger()
    logger.info("Performing pipeline health check")
    
    health_status = {
        "timestamp": datetime.now().isoformat(),
        "overall_status": "healthy",
        "components": {},
        "alerts": []
    }
    
    # Check data pipeline health
    data_health = {
        "status": "healthy",
        "issues": []
    }
    
    if not os.path.exists("heart_processed.csv"):
        data_health["status"] = "unhealthy"
        data_health["issues"].append("Processed data file not found")
        health_status["alerts"].append("Data pipeline: No processed data available")
    else:
        # Check file age
        file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime("heart_processed.csv"))
        if file_age > timedelta(hours=24):
            data_health["issues"].append("Data file is older than 24 hours")
            health_status["alerts"].append("Data pipeline: Data may be stale")
    
    health_status["components"]["data_pipeline"] = data_health
    
    # Check ML pipeline health
    ml_health = {
        "status": "healthy",
        "issues": []
    }
    
    if not os.path.exists("models"):
        ml_health["status"] = "unhealthy"
        ml_health["issues"].append("Models directory not found")
        health_status["alerts"].append("ML pipeline: No trained models available")
    else:
        model_files = [f for f in os.listdir("models") if f.endswith('.pkl')]
        if not model_files:
            ml_health["status"] = "unhealthy"
            ml_health["issues"].append("No trained models found")
            health_status["alerts"].append("ML pipeline: No trained models found")
    
    health_status["components"]["ml_pipeline"] = ml_health
    
    # Check API health
    api_health = {
        "status": "healthy",
        "issues": []
    }
    
    # This would typically check if the API is responding
    # For now, we'll assume it's healthy if the integration file exists
    if not os.path.exists("prefect_api_integration.py"):
        api_health["status"] = "unhealthy"
        api_health["issues"].append("API integration file not found")
        health_status["alerts"].append("API: Integration file missing")
    
    health_status["components"]["api"] = api_health
    
    # Determine overall status
    component_statuses = [comp["status"] for comp in health_status["components"].values()]
    if "unhealthy" in component_statuses:
        health_status["overall_status"] = "degraded"
    if len(health_status["alerts"]) > 2:
        health_status["overall_status"] = "critical"
    
    logger.info(f"Health check completed. Overall status: {health_status['overall_status']}")
    logger.info(f"Alerts: {len(health_status['alerts'])}")
    
    return health_status


@task(name="send_alerts", retries=1)
def send_alerts(health_status: Dict[str, Any]) -> bool:
    """
    Send alerts based on health status
    """
    logger = get_run_logger()
    logger.info("Processing alerts")
    
    if not health_status["alerts"]:
        logger.info("No alerts to send")
        return True
    
    # In a real implementation, this would send emails, Slack messages, etc.
    # For now, we'll just log the alerts
    alert_file = f"alerts/alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    os.makedirs("alerts", exist_ok=True)
    
    with open(alert_file, 'w') as f:
        f.write(f"Pipeline Health Alerts - {datetime.now().isoformat()}\n")
        f.write("=" * 50 + "\n")
        f.write(f"Overall Status: {health_status['overall_status']}\n\n")
        
        for i, alert in enumerate(health_status["alerts"], 1):
            f.write(f"{i}. {alert}\n")
    
    logger.warning(f"Alerts generated: {len(health_status['alerts'])} alerts")
    logger.info(f"Alert file saved to: {alert_file}")
    
    return True


@flow(
    name="heart-disease-monitoring-pipeline",
    description="Comprehensive monitoring and alerting pipeline for heart disease ML system"
)
def heart_disease_monitoring_pipeline() -> Dict[str, Any]:
    """
    Main monitoring pipeline that collects metrics, generates reports, and checks health
    """
    logger = get_run_logger()
    logger.info("Starting Heart Disease Monitoring Pipeline")
    
    # Collect metrics
    metrics = collect_pipeline_metrics()
    
    # Generate performance report
    report_path = generate_performance_report(metrics)
    
    # Create monitoring dashboard
    dashboard_path = create_monitoring_dashboard(metrics)
    
    # Check pipeline health
    health_status = check_pipeline_health()
    
    # Send alerts if needed
    alerts_sent = send_alerts(health_status)
    
    # Summary
    monitoring_summary = {
        "timestamp": datetime.now().isoformat(),
        "metrics_collected": len(metrics),
        "performance_report": report_path,
        "monitoring_dashboard": dashboard_path,
        "health_status": health_status["overall_status"],
        "alerts_count": len(health_status["alerts"]),
        "alerts_sent": alerts_sent,
        "status": "completed"
    }
    
    logger.info("Monitoring pipeline completed successfully!")
    logger.info(f"Summary: {monitoring_summary}")
    
    return monitoring_summary


# Setup function for logging
def initialize_monitoring():
    """
    Initialize the monitoring system
    """
    data_logger, ml_logger, api_logger, monitor_logger = setup_advanced_logging()
    
    print("""
    ========================================
    Heart Disease ML Pipeline Monitoring
    ========================================
    
    Monitoring system initialized with:
    - Advanced logging configuration
    - Performance metrics collection
    - Health check monitoring
    - Alert system
    - Visual dashboard generation
    
    Log files location: ./logs/
    Reports location: ./reports/
    Dashboard location: ./dashboard/
    Alerts location: ./alerts/
    
    To run monitoring:
    python prefect_monitoring.py --monitor
    
    ========================================
    """)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Prefect Monitoring System")
    parser.add_argument("--monitor", action="store_true", help="Run monitoring pipeline")
    parser.add_argument("--init", action="store_true", help="Initialize monitoring system")
    
    args = parser.parse_args()
    
    if args.init:
        initialize_monitoring()
    elif args.monitor:
        print("Starting monitoring pipeline...")
        result = heart_disease_monitoring_pipeline()
        print(f"Monitoring completed: {result}")
    else:
        print("""
        Heart Disease ML Pipeline Monitoring System
        
        Usage:
        python prefect_monitoring.py --init     # Initialize monitoring system
        python prefect_monitoring.py --monitor  # Run monitoring pipeline
        
        Features:
        - Comprehensive metrics collection
        - Performance reporting
        - Health checking
        - Alert system
        - Visual dashboard generation
        """)
