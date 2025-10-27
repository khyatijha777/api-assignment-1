"""
Prefect Scheduling and Deployment Configuration
Sets up automated scheduling and deployment for the heart disease ML pipeline
"""

from prefect import flow, serve
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule
from datetime import timedelta
import os

# Import our pipeline flows
from prefect_data_pipeline import heart_disease_data_pipeline
from prefect_ml_pipeline import heart_disease_ml_pipeline
from prefect_api_integration import api_status_pipeline, update_pipeline_status


@flow(name="heart-disease-orchestrator")
def heart_disease_orchestrator():
    """
    Orchestrator flow that runs the complete pipeline
    """
    print("Starting Heart Disease ML Pipeline Orchestrator")
    
    try:
        # Update status
        update_pipeline_status("data_pipeline", "running")
        
        # Run data pipeline
        print("Running data preprocessing pipeline...")
        data_result = heart_disease_data_pipeline()
        update_pipeline_status("data_pipeline", "completed", data_result)
        
        # Update status
        update_pipeline_status("ml_pipeline", "running")
        
        # Run ML pipeline
        print("Running ML training pipeline...")
        ml_result = heart_disease_ml_pipeline()
        update_pipeline_status("ml_pipeline", "completed", ml_result)
        
        print("Complete pipeline finished successfully!")
        return {
            "data_pipeline": data_result,
            "ml_pipeline": ml_result,
            "status": "completed"
        }
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        update_pipeline_status("data_pipeline", "failed", {"error": str(e)})
        update_pipeline_status("ml_pipeline", "failed", {"error": str(e)})
        raise


# Create deployments
def create_deployments():
    """
    Create Prefect deployments for different scheduling scenarios
    """
    
    # Deployment 1: Data Pipeline - Runs every 2 minutes (as per original requirement)
    data_deployment = Deployment.build_from_flow(
        flow=heart_disease_data_pipeline,
        name="heart-disease-data-pipeline-deployment",
        description="Heart Disease Data Preprocessing Pipeline - Runs every 2 minutes",
        schedule=IntervalSchedule(interval=timedelta(minutes=2)),
        parameters={"input_file": "heart.csv", "output_file": "heart_processed.csv"},
        tags=["data", "preprocessing", "heart-disease"],
        work_pool_name="default-agent-pool"
    )
    
    # Deployment 2: ML Pipeline - Runs daily at 6 AM
    ml_deployment = Deployment.build_from_flow(
        flow=heart_disease_ml_pipeline,
        name="heart-disease-ml-pipeline-deployment",
        description="Heart Disease ML Training Pipeline - Runs daily at 6 AM",
        schedule=CronSchedule(cron="0 6 * * *"),  # Daily at 6 AM
        parameters={
            "processed_data_file": "heart_processed.csv",
            "test_size": 0.3,
            "random_state": 42,
            "models_dir": "models"
        },
        tags=["ml", "training", "heart-disease"],
        work_pool_name="default-agent-pool"
    )
    
    # Deployment 3: Complete Orchestrator - Runs every 4 hours
    orchestrator_deployment = Deployment.build_from_flow(
        flow=heart_disease_orchestrator,
        name="heart-disease-orchestrator-deployment",
        description="Complete Heart Disease Pipeline Orchestrator - Runs every 4 hours",
        schedule=IntervalSchedule(interval=timedelta(hours=4)),
        tags=["orchestrator", "complete-pipeline", "heart-disease"],
        work_pool_name="default-agent-pool"
    )
    
    # Deployment 4: API Status Pipeline - Runs every 5 minutes for monitoring
    api_deployment = Deployment.build_from_flow(
        flow=api_status_pipeline,
        name="heart-disease-api-status-deployment",
        description="API Status Pipeline for Monitoring - Runs every 5 minutes",
        schedule=IntervalSchedule(interval=timedelta(minutes=5)),
        tags=["api", "monitoring", "status"],
        work_pool_name="default-agent-pool"
    )
    
    return [data_deployment, ml_deployment, orchestrator_deployment, api_deployment]


def create_manual_deployments():
    """
    Create manual deployments for on-demand execution
    """
    
    # Manual Data Pipeline
    manual_data_deployment = Deployment.build_from_flow(
        flow=heart_disease_data_pipeline,
        name="heart-disease-data-pipeline-manual",
        description="Manual execution of Heart Disease Data Pipeline",
        tags=["data", "preprocessing", "manual", "heart-disease"],
        work_pool_name="default-agent-pool"
    )
    
    # Manual ML Pipeline
    manual_ml_deployment = Deployment.build_from_flow(
        flow=heart_disease_ml_pipeline,
        name="heart-disease-ml-pipeline-manual",
        description="Manual execution of Heart Disease ML Pipeline",
        tags=["ml", "training", "manual", "heart-disease"],
        work_pool_name="default-agent-pool"
    )
    
    # Manual Orchestrator
    manual_orchestrator_deployment = Deployment.build_from_flow(
        flow=heart_disease_orchestrator,
        name="heart-disease-orchestrator-manual",
        description="Manual execution of Complete Heart Disease Pipeline",
        tags=["orchestrator", "manual", "heart-disease"],
        work_pool_name="default-agent-pool"
    )
    
    return [manual_data_deployment, manual_ml_deployment, manual_orchestrator_deployment]


def setup_prefect_server():
    """
    Setup instructions for Prefect server
    """
    print("""
    ========================================
    Prefect Server Setup Instructions
    ========================================
    
    1. Start Prefect Server:
       prefect server start
    
    2. In another terminal, start a work pool agent:
       prefect agent start --pool default-agent-pool
    
    3. Apply deployments:
       python prefect_deployment.py --apply
    
    4. View Prefect UI:
       Open http://localhost:4200 in your browser
    
    5. Start API server:
       python prefect_api_integration.py
    
    ========================================
    """)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Prefect Deployment Manager")
    parser.add_argument("--apply", action="store_true", help="Apply deployments to Prefect server")
    parser.add_argument("--manual", action="store_true", help="Create manual deployments only")
    parser.add_argument("--setup", action="store_true", help="Show setup instructions")
    
    args = parser.parse_args()
    
    if args.setup:
        setup_prefect_server()
    elif args.manual:
        print("Creating manual deployments...")
        deployments = create_manual_deployments()
        for deployment in deployments:
            deployment.apply()
        print("Manual deployments created successfully!")
    elif args.apply:
        print("Creating and applying scheduled deployments...")
        deployments = create_deployments()
        for deployment in deployments:
            deployment.apply()
        print("Scheduled deployments created and applied successfully!")
        print("\nTo start the Prefect server, run: prefect server start")
        print("To start an agent, run: prefect agent start --pool default-agent-pool")
    else:
        print("""
        Prefect Deployment Manager for Heart Disease ML Pipeline
        
        Usage:
        python prefect_deployment.py --apply     # Create and apply scheduled deployments
        python prefect_deployment.py --manual   # Create manual deployments only
        python prefect_deployment.py --setup    # Show setup instructions
        
        Available Deployments:
        1. Data Pipeline (every 2 minutes)
        2. ML Pipeline (daily at 6 AM)
        3. Complete Orchestrator (every 4 hours)
        4. API Status Pipeline (every 5 minutes)
        """)
